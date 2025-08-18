using Cassandra.DataApi;
using Cassandra.DataApi.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;

class Programs
{
    static async Task Main()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var endpoint = config["Astra:Endpoint"];
        var token    = config["Astra:Token"];

        var client = new AstraDataApiClient(endpoint, token);

        var usersJson = await client.GetAllRowsAsync("users");
        Console.WriteLine("Users: " + usersJson);

        var user = new User
        {
            email = "siva@example.com",
            name = "Siva",
            user_id = Guid.NewGuid()
        };
        var result = await client.InsertRowAsync("users", user);
        Console.WriteLine("Insert result: " + result);
    }
}
