using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace RavenIndexInvestigation2
{
    class Program
    {
        static void Main(string[] args)
        {
            var documentStore = new DocumentStore {Url = "http://localhost:8080"};
            documentStore.Initialize();
            while (true)
            {
                var trigger = new PermissionTreeIndexTrigger("tenancy-pbw", documentStore);
                trigger.Trigger().Wait();
                Thread.Sleep(TimeSpan.FromMilliseconds(100));
            }
        }
    }
    internal interface IIndexTrigger
    {
        string IndexName { get; }
        Task Trigger();
    }
    public class PermissionTreeIndexTrigger : BaseAsyncIndexTrigger
    {
        private readonly string _dbName;
        private readonly IDocumentStore _documentStore;
        private readonly string _indexName;

        public PermissionTreeIndexTrigger(string dbName, IDocumentStore documentStore)
            : base(dbName, documentStore)
        {
            _dbName = dbName;
            _documentStore = documentStore;
            _indexName = new PermissionTreeIndex().IndexName;
        }

        public override string IndexName { get { return _indexName; } }

        protected override string[] IndexFields
        {
            get
            {
                return new[]
                    {
                        "__document_id", 
                        PermissionTreeIndex.StateDocFieldName,
                        PermissionTreeIndex.DependsOnFieldName
                    };
            }
        }

        protected override Func<IEnumerable<ICommandData>> ProcessEntry(RavenJObject entry)
        {
            var documentId = entry.Value<string>("__document_id");
            var stateDocId = entry.Value<string>(PermissionTreeIndex.StateDocFieldName);
            var dependsDocs = (entry.Value<RavenJArray>(PermissionTreeIndex.DependsOnFieldName) ?? new RavenJArray()).Select(x => x.Value<string>());

            return () => UpdateDependencies(documentId, stateDocId, dependsDocs);
        }

        private IEnumerable<ICommandData> UpdateDependencies(string documentId, string stateDocId, IEnumerable<string> dependsDocIds)
        {
            var databaseCommands = _documentStore.DatabaseCommands.ForDatabase(_dbName);
            var docIds = dependsDocIds as string[] ?? dependsDocIds.ToArray();
            var ids = docIds.Concat(new[] { documentId, stateDocId }).Where(x => x != null).ToArray();
            var results =
                databaseCommands
                .Get(ids, new string[0])
                .Results.Select(SerializationHelper.ToJsonDocument)
                .Where(x => x != null)
                .ToArray();
            var stateDoc = GetDocument(results, stateDocId).SingleOrDefault();
            var nodeDoc = DeserializeJsonDocument<PermissionTreeNode>(GetDocument(results, documentId).SingleOrDefault());

            var dependDocs = docIds
                .SelectMany(id => GetDocument(results, id))
                .Select(DeserializeJsonDocument<PermissionGroupState>)
                .ToList();

            Etag stateEtag;
            PermissionGroupState stateDocument;
            RavenJObject metadata;

            if (stateDoc == null)
            {
                stateDocument = new PermissionGroupState
                {
                    Id = nodeDoc.StateId,
                    Version = 0,
                    VersionDispatched = 0
                };

                metadata = new RavenJObject();
                metadata["Raven-Entity-Name"] = new RavenJValue("PermissionGroupStates");
                metadata["Raven-Clr-Type"] = new RavenJValue("Dust.Authorization.PermissionGroupState, Dust");

                stateEtag = Etag.Empty;
            }
            else
            {
                stateDocument = DeserializeJsonDocument<PermissionGroupState>(stateDoc);
                metadata = new RavenJObject();
                metadata["Raven-Entity-Name"] = new RavenJValue("PermissionGroupStates");
                metadata["Raven-Clr-Type"] = new RavenJValue("Dust.Authorization.PermissionGroupState, Dust");
                stateEtag = stateDoc.Etag;
                stateDocument.Version++;
            }

            stateDocument.AllChildren = dependDocs.SelectMany(x => x.Children).ToList();
            stateDocument.AllUsers = nodeDoc.Users.Select(x => x.ToString()).ToList();
            stateDocument.UserGroupContainer = nodeDoc.UserGroupContainer;
            stateDocument.EntityId = nodeDoc.EntityId;
            stateDocument.NodeId = documentId;
            stateDocument.Role = nodeDoc.Role;
            stateDocument.UserGroupContainer = nodeDoc.UserGroupContainer;

            var children = new HashSet<string>();
            var users = new HashSet<string>();

            foreach (var childState in dependDocs)
            {
                childState.Children.ForEach(x => children.Add(x));
                if (stateDocument.UserGroupContainer)
                {
                    childState.Users.ForEach(x => users.Add(x));
                }
            }

            if (stateDocument.UserGroupContainer)
            {
                children.Add(stateDocId);
                nodeDoc.Users.ForEach(x => users.Add(x.ToString()));
            }
            else
            {
                nodeDoc.Users.ForEach(userId => children.Add(string.Format("PermissionGroupState/{0}/Me", userId)));
            }

            stateDocument.Children = children.ToList();
            stateDocument.Users = users.ToList();

            return new ICommandData[]
                {
                    new PutCommandData
                        {
                            Key = stateDocId,
                            Etag = stateEtag,
                            Document = RavenJObject.FromObject(stateDocument),
                            Metadata = metadata
                        }
                };
        }

        private static T DeserializeJsonDocument<T>(JsonDocument jsonDocument)
        {
            return JsonConvert.DeserializeObject<T>(jsonDocument.ToJson().ToString());
        }
    }
    public class PermissionGroupState
    {
        public string Id { get; set; }

        public PermissionGroupState()
        {
            Children = new List<string>();
            Users = new List<string>();

            AllChildren = new List<string>();
            AllUsers = new List<string>();
        }

        public string NodeId { get; set; }

        public List<string> Children { get; set; }

        public List<string> Users { get; set; }

        public Guid EntityId { get; set; }

        public string Role { get; set; }

        public long Version { get; set; }

        public long VersionDispatched { get; set; }

        public bool UserGroupContainer { get; set; }

        public List<string> AllChildren { get; set; }

        public List<string> AllUsers { get; set; }
    }

    public abstract class BaseAsyncIndexTrigger : IIndexTrigger
    {
        private readonly string _dbName;
        private readonly IDocumentStore _documentStore;

        protected BaseAsyncIndexTrigger(string dbName, IDocumentStore documentStore)
        {
            _dbName = dbName;
            _documentStore = documentStore;
        }

        public abstract string IndexName { get; }
        protected abstract string[] IndexFields { get; }
        protected abstract Func<IEnumerable<ICommandData>> ProcessEntry(RavenJObject entry);
        public async Task Trigger()
        {
            var tasks = new List<Task<IEnumerable<ICommandData>>>();

            var databaseCommands = _documentStore.DatabaseCommands.ForDatabase(_dbName);
            await Task.Run(() =>
            {
                QueryHeaderInformation queryHeaderInformation;
                var indexQuery = new IndexQuery { FieldsToFetch = IndexFields };
                var enumerable = databaseCommands.StreamQuery(IndexName, indexQuery, out queryHeaderInformation);

                while (enumerable.MoveNext())
                {
                    var ravenJObject = enumerable.Current;
                    tasks.Add(Task.Run(() => CreateTaskForEntry(ravenJObject)));
                }
            });

            await Task
                    .WhenAll(tasks)
                    .ContinueWith(results =>
                    {
                        var commands = results.Result.ToArray();
                        databaseCommands.Batch(commands.SelectMany(x => x));
                    });
        }

        private IEnumerable<ICommandData> CreateTaskForEntry(RavenJObject ravenJObject)
        {
            try
            {
                return ProcessEntry(ravenJObject)();
            }
            catch (Exception ex)
            {
                Log(ex);
                return new ICommandData[0];
            }
        }

        private void Log(Exception exception)
        {
            Console.WriteLine("Exception: {0}", exception.Message);
        }

        public static IEnumerable<JsonDocument> GetDocument(JsonDocument[] results, string id)
        {
            return results.Where(x => String.Equals(x.Key, id, StringComparison.InvariantCultureIgnoreCase));
        }
    }

    public class PermissionTreeNode
    {
        public PermissionTreeNode()
        {
            ImmediateChildren = new HashSet<string>();
            Users = new HashSet<Guid>();
        }

        public string Id { get; set; }
        public string StateId { get; set; }

        // is this node a user group container - (org, user group, all users etc?)
        // if it's a user group container then the user's arn't inherited 
        // down - instead this group is stored with the user
        public bool UserGroupContainer = false;

        public Guid EntityId { get; set; }
        public string Role { get; set; }
        public HashSet<string> ImmediateChildren { get; set; }
        public HashSet<Guid> Users { get; set; }
    }

    public class PermissionTreeIndex : AbstractIndexCreationTask<PermissionTreeNode, PermissionTreeIndex.Result>
    {
        public const string DependsOnFieldName = "DependsOn";
        public const string StateDocFieldName = "StateDoc";

        public class Result
        {
            public string Id { get; set; }
            public List<string> DependsOn { get; set; }
            public string StateDoc { get; set; }
        }

        public PermissionTreeIndex()
        {

            Map = items => items
                               .Select(x =>
                                       new
                                       {
                                           x.Id,
                                           x.EntityId,
                                           x.Role,
                                           x.StateId,
                                           x.ImmediateChildren,
                                           x.Users,
                                           x.UserGroupContainer,
                                           CurrentState = LoadDocument<PermissionGroupState>(x.StateId),
                                           AllChildren = x
                                               .ImmediateChildren
                                               .SelectMany(c => LoadDocument<PermissionGroupState>(c).Children),
                                           AllUsers = x.Users.Select(u => u.ToString())
                                       }
                               )
                // Raven indexes are special
                // ReSharper disable SuspiciousTypeConversion.Global 
                // ReSharper disable PossibleUnintendedReferenceComparison
                               .Where(x =>
                                   x.CurrentState == null
                                   ||
                                   Enumerable.Union(Enumerable.Except(x.CurrentState.AllChildren, x.AllChildren), Enumerable.Except(x.AllChildren, x.CurrentState.AllChildren)).Any()
                                   ||
                                   x.EntityId != x.CurrentState.EntityId
                                   ||
                                   x.Role.ToString() != x.CurrentState.Role.ToString()
                                   ||
                                   Enumerable.Union(Enumerable.Except(x.CurrentState.AllUsers, x.AllUsers), Enumerable.Except(x.AllUsers, x.CurrentState.AllUsers)).Any()
                                   )
                // ReSharper restore PossibleUnintendedReferenceComparison
                // ReSharper restore SuspiciousTypeConversion.Global
                               .Select(x => new
                               {
                                   Id = x.Id,
                                   CurrentStateAllChildren = x.CurrentState.AllChildren,
                                   CurrentStateRole = x.CurrentState.Role.ToString(),
                                   CurrentStateEntityId = x.CurrentState.EntityId,
                                   CurrentStateAllUsers = x.CurrentState.AllUsers,
                                   x.AllUsers,
                                   x.AllChildren,
                                   Role = x.Role.ToString(),
                                   x.EntityId,
                                   DependsOn = x.ImmediateChildren.OrderBy(y => y).ToList(), //.Select(y => y.DocId).ToList(),
                                   StateDoc = x.StateId
                               });

            StoresStrings.Add("StateDoc", FieldStorage.Yes);
            StoresStrings.Add("DependsOn", FieldStorage.Yes);
        }
    }

}
