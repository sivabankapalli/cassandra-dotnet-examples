using System;
using System.IO;
using System.Threading.Tasks;
using Cassandra.SecureConnect;
using Microsoft.Extensions.Configuration;

static class Program
{
    static async Task Main()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var scbPathSetting = config["Astra:SecureConnectBundlePath"];
        var scbPath = Path.IsPathFullyQualified(scbPathSetting)
            ? scbPathSetting
            : Path.Combine(AppContext.BaseDirectory, scbPathSetting);

        var token   = config["Astra:Token"];
        var keyspace= config["Astra:Keyspace"];

        var session = await CassandraSessionFactory.CreateSessionAsync(scbPath, token, keyspace);
        Console.WriteLine("Connected!");

        var repo = new ChatRepository(session);
        await repo.InitAsync();
        await repo.PrintUserAsync("fred@qmail.net");
    }
}
