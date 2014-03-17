using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Smuggler;

namespace RavenIndexInvestigation2
{
    class Program
    {
        private const string RavenServerUrl = "http://localhost:8080";
        const string DatabaseName = "foo8";
        static void Main()
        {
            var documentStore = new DocumentStore { Url = RavenServerUrl };
            documentStore.Initialize();


            // creates the database with replication bundle
            // this bug can only be replicated when the replication bundle is configured
            CreateDatabase(documentStore);

            ImportData();

            var databaseCommands = documentStore.DatabaseCommands.ForDatabase(DatabaseName);

            EnsureIndexExists(databaseCommands);

            while (true)
            {
                try
                {
                    var trigger = new PermissionGroupStateUpdater(DatabaseName, documentStore);
                    trigger.Trigger();
                }
                catch
                {
                    // don't worry about this for now just retry
                }
                Thread.Sleep(TimeSpan.FromMilliseconds(100));
            }
        }

        private static void EnsureIndexExists(IDatabaseCommands databaseCommands)
        {
            var indexNames = databaseCommands.GetIndexes(0, 10000);
            var permissionTreeIndex = new PermissionTreeIndex();
            if (indexNames.All(x => x.Name != permissionTreeIndex.IndexName))
            {
                databaseCommands
                    .PutIndex(permissionTreeIndex.IndexName,
                              permissionTreeIndex.CreateIndexDefinition());
            }
        }

        private static void ImportData()
        {
            var connectionStringOptions = new RavenConnectionStringOptions
                {
                    DefaultDatabase = DatabaseName,
                    Url = RavenServerUrl,
                };

            var smugglerApi = new SmugglerApi(new SmugglerOptions(), connectionStringOptions);
            var result = smugglerApi.ImportData(
                                   new SmugglerOptions
                                       {
                                           BackupPath = "PermissionTreeNodes.dump.raven",
                                           OperateOnTypes = ItemType.Documents
                                       });
            result.Wait();
        }

        private static void CreateDatabase(DocumentStore documentStore)
        {
            var databaseDocument = new DatabaseDocument
                {
                    Disabled = false,
                    Id = DatabaseName,
                    Settings = new Dictionary<string, string>
                        {
                            {"Raven/DataDir", string.Format(@"~\Databases\{0}", DatabaseName)},
                            {"Raven/ActiveBundles", "Replication"}
                        },
                    SecuredSettings = new Dictionary<string, string>()
                };

            documentStore.DatabaseCommands.CreateDatabase(databaseDocument);
        }
    }
}
