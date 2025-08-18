using System;

namespace Cassandra.DataApi.Models
{
    public class User
    {
        public string email { get; set; }
        public string name { get; set; }
        public Guid user_id { get; set; }
    }
}
