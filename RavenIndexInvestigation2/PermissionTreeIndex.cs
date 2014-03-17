using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace RavenIndexInvestigation2
{
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