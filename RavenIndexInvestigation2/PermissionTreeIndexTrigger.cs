using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace RavenIndexInvestigation2
{
    public class PermissionGroupStateUpdater
    {
        private readonly string _dbName;
        private readonly IDocumentStore _documentStore;
        private readonly string _indexName;

        public PermissionGroupStateUpdater(string dbName, IDocumentStore documentStore)
        {
            _dbName = dbName;
            _documentStore = documentStore;
            _indexName = new PermissionTreeIndex().IndexName;
        }

        public string IndexName { get { return _indexName; } }

        protected string[] IndexFields
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

        protected Func<IEnumerable<ICommandData>> ProcessEntry(RavenJObject entry)
        {
            var documentId = entry.Value<string>("__document_id");
            var stateDocId = entry.Value<string>(PermissionTreeIndex.StateDocFieldName);
            var dependsDocs = (entry.Value<RavenJArray>(PermissionTreeIndex.DependsOnFieldName) ?? new RavenJArray()).Select(x => x.Value<string>());

            return () => UpdateDependencies(documentId, stateDocId, dependsDocs);
        }

        private IEnumerable<ICommandData> UpdateDependencies(string treeNodeDocumentId, string stateDocumentId, IEnumerable<string> dependsDocIds)
        {
            var databaseCommands = _documentStore.DatabaseCommands.ForDatabase(_dbName);
            var docIds = dependsDocIds as string[] ?? dependsDocIds.ToArray();
            var ids = docIds.Concat(new[] { treeNodeDocumentId, stateDocumentId }).Where(x => x != null).ToArray();
            var results =
                databaseCommands
                    .Get(ids, new string[0])
                    .Results.Select(SerializationHelper.ToJsonDocument)
                    .Where(x => x != null)
                    .ToArray();
            var jsonStateDocument = GetDocument(results, stateDocumentId).SingleOrDefault();
            var treeNodeDocument = DeserializeJsonDocument<PermissionTreeNode>(GetDocument(results, treeNodeDocumentId).SingleOrDefault());

            // get child states
            var dependDocs = docIds
                .SelectMany(id => GetDocument(results, id))
                .Select(DeserializeJsonDocument<PermissionGroupState>)
                .ToList();

            Etag stateEtag;
            PermissionGroupState stateDocument;
            RavenJObject metadata;

            if (jsonStateDocument == null)
            {
                stateDocument = new PermissionGroupState
                    {
                        Id = treeNodeDocument.StateId,
                    };

                metadata = new RavenJObject();
                stateEtag = Etag.Empty;
            }
            else
            {
                stateDocument = DeserializeJsonDocument<PermissionGroupState>(jsonStateDocument);
                metadata = new RavenJObject();
                stateEtag = jsonStateDocument.Etag;
            }

            metadata["Raven-Entity-Name"] = new RavenJValue("PermissionGroupStates");
            stateDocument.AllChildren = dependDocs.SelectMany(x => x.Children).ToList();
            stateDocument.AllUsers = treeNodeDocument.Users.Select(x => x.ToString()).ToList();
            stateDocument.UserGroupContainer = treeNodeDocument.UserGroupContainer;
            stateDocument.EntityId = treeNodeDocument.EntityId;
            stateDocument.NodeId = treeNodeDocumentId;
            stateDocument.Role = treeNodeDocument.Role;
            stateDocument.UserGroupContainer = treeNodeDocument.UserGroupContainer;

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
                children.Add(stateDocumentId);
                treeNodeDocument.Users.ForEach(x => users.Add(x.ToString()));
            }
            else
            {
                treeNodeDocument.Users.ForEach(userId => children.Add(string.Format("PermissionGroupState/{0}/Me", userId)));
            }

            stateDocument.Children = children.ToList();
            stateDocument.Users = users.ToList();

            return new ICommandData[]
                {
                    new PutCommandData
                        {
                            Key = stateDocumentId,
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

        public void Trigger()
        {
            var databaseCommands = _documentStore.DatabaseCommands.ForDatabase(_dbName);

            QueryHeaderInformation queryHeaderInformation;
            var indexQuery = new IndexQuery { FieldsToFetch = IndexFields };
            var enumerable = databaseCommands.StreamQuery(IndexName, indexQuery, out queryHeaderInformation);

            var commands = new List<ICommandData>();
            while (enumerable.MoveNext())
            {
                var ravenJObject = enumerable.Current;
                commands.AddRange(CreateTaskForEntry(ravenJObject));
            }

            databaseCommands.Batch(commands);
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
}