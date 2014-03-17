using System;
using System.Collections.Generic;

namespace RavenIndexInvestigation2
{
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

        public bool UserGroupContainer { get; set; }

        public List<string> AllChildren { get; set; }

        public List<string> AllUsers { get; set; }
    }
}