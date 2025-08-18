using Cassandra.DataApi;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
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
        var config = LoadConfiguration();

        var endpoint = config["Astra:Endpoint"] ?? throw new InvalidOperationException("Astra:Endpoint is missing in configuration.");
        var token = config["Astra:Token"] ?? throw new InvalidOperationException("Astra:Token is missing in configuration.");
        const string keyspace = "dev_cdk_ks";

        using var client = new AstraDataApiClient(token, endpoint);
        var database = client.GetDatabase(keyspace);
        var usersCollection = database.GetCollection<UserDoc>("users");

        var newUser = new UserDoc
        {
            email = "siva@example.com",
            name = "Siva",
            password = "password123",
            user_id = Guid.NewGuid()
        };

        var insertedId = await usersCollection.InsertOneAsync(newUser);
        Console.WriteLine($"InsertedId: {insertedId}");

        var foundUser = await usersCollection.FindOneAsync(new { email = newUser.email });
        Console.WriteLine(foundUser is null
            ? "Not found"
            : $"Found: {foundUser.email} / {foundUser.name}");
    }

    static IConfiguration LoadConfiguration() =>
        new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
}
