using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace Cassandra.SecureConnect
{
    public class ChatRepository
    {
        private readonly ISession _session;
        private PreparedStatement _getUserByEmail;

        public ChatRepository(ISession session) { _session = session; }

        public async Task InitAsync()
        {
            _getUserByEmail = await _session.PrepareAsync("SELECT email, name, user_id FROM users WHERE email = ?");
        }

        public async Task PrintUserAsync(string email)
        {
            var rs = await _session.ExecuteAsync(_getUserByEmail.Bind(email).SetIdempotence(true));
            var row = rs.FirstOrDefault();
            if (row == null)
                Console.WriteLine("No user found.");
            else
                Console.WriteLine($"User: {row.GetValue<string>("name")} <{row.GetValue<string>("email")}>");
        }
    }
}
