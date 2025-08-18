using System.Threading.Tasks;
using Cassandra;

namespace Cassandra.SecureConnect
{
    public static class CassandraSessionFactory
    {
        public static async Task<ISession> CreateSessionAsync(string scbPath, string token, string keyspace)
        {
            var cluster = Cluster.Builder()
                .WithCloudSecureConnectionBundle(scbPath)
                .WithCredentials("token", token)
                .Build();

            return await cluster.ConnectAsync(keyspace);
        }
    }
}
