using Cassandra.DataApi;
using System;
using System.Threading.Tasks;

public class UserDoc
{
    public string email { get; set; } = default!;
    public string name { get; set; } = default!;
    public string password { get; set; } = default!;

    public Guid user_id { get; set; }
}

class Program
{
    static async Task Main()
    {
        string token = "";
        string endpoint = "https://3fc2f1fb-ef2f-4f80-ad2b-b8ac0c97ce93-westus3.apps.astra.datastax.com";
        string keyspace = "dev_cdk_ks";

        using var client = new AstraDataApiClient(token, endpoint);
        var database = client.GetDatabase(keyspace);
        var users = database.GetCollection<UserDoc>("users");

        // Insert one
        var insert = await users.InsertOneAsync(new UserDoc
        {
            email = "siva@example.com",
            name = "Siva",
            password = "password123",
            user_id = Guid.NewGuid()
        });

        Console.WriteLine("InsertedId: " + insert);

        // Find one
        var found = await users.FindOneAsync(new { email = "siva@example.com" });
        Console.WriteLine(found is null ? "Not found" : $"Found: {found.email} / {found.name}");
    }
}
