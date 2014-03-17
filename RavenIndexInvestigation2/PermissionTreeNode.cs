using System;
using System.Collections.Generic;

namespace RavenIndexInvestigation2
{
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
}