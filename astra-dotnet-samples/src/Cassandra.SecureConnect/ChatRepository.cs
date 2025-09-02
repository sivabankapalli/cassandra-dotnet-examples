using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace MiniCrudFinal;

internal static class Program
{
    static async Task<int> Main()
    {
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        try
        {
            var cfg = Settings.Load();
            Validate(cfg);

            await using var crud = await UserCrud.ConnectAsync(cfg.ScbPath, cfg.Token, cfg.Keyspace, cts.Token);

            // OPTIONAL: create tables if they don't exist (safe to leave on)
            await crud.EnsureSchemaAsync(cts.Token);

            // --- Demo flow ----------------------------------------------------
            var u = new User(Guid.NewGuid(), "ada@example.com", "Ada Lovelace", "secret");

            await crud.InsertAsync(u, cts.Token);
            Info("Inserted:");
            Table.Print(new[] { u }, ("user_id", x => x.UserId), ("email", x => x.Email), ("name", x => x.Name));

            var byId = await crud.GetByIdAsync(u.UserId, cts.Token);
            Info("\nFetched by ID:");
            Table.Print(Arr(byId), ("user_id", x => x.UserId), ("email", x => x.Email), ("name", x => x.Name));

            var byEmail = await crud.GetByEmailAsync(u.Email, cts.Token);
            Info("\nFetched by Email (via users_by_email):");
            Table.Print(Arr(byEmail), ("user_id", x => x.UserId), ("email", x => x.Email), ("name", x => x.Name));

            await crud.UpdateNameAsync(u.UserId, u.Email, "Ada L.", cts.Token);
            var after = await crud.GetByIdAsync(u.UserId, cts.Token);
            Info("\nAfter Update:");
            Table.Print(Arr(after), ("user_id", x => x.UserId), ("email", x => x.Email), ("name", x => x.Name));

            await crud.DeleteByIdAsync(u.UserId, cts.Token);
            var gone = await crud.GetByIdAsync(u.UserId, cts.Token);
            Info("\nAfter Delete:");
            Table.Print(Arr(gone), ("user_id", x => x.UserId), ("email", x => x.Email), ("name", x => x.Name));

            return 0;
        }
        catch (OperationCanceledException) { Warn("Cancelled."); return 1; }
        catch (AuthenticationException ex) { Error("Authentication failed: " + ex.Message); return 2; }
        catch (NoHostAvailableException ex)
        {
            Error("NoHostAvailable: " + ex.Message);
            foreach (var kv in ex.Errors) Console.WriteLine($"  {kv.Key}: {kv.Value.GetType().Name} - {kv.Value.Message}");
            return 3;
        }
        catch (InvalidQueryException ex) { Error("InvalidQuery: " + ex.Message); return 4; }
        catch (Exception ex) { Error(ex.ToString()); return 5; }

        static void Validate(Settings s)
        {
            if (string.IsNullOrWhiteSpace(s.ScbPath) || !File.Exists(s.ScbPath))
                throw new FileNotFoundException($"SCB not found: {s.ScbPath}");
            if (string.IsNullOrWhiteSpace(s.Token)) throw new ArgumentException("Token is empty.");
            if (string.IsNullOrWhiteSpace(s.Keyspace)) throw new ArgumentException("Keyspace is empty.");
        }
        static User[] Arr(User? u) => u is null ? Array.Empty<User>() : new[] { u };
        static void Info(string m) => Console.WriteLine(m);
        static void Warn(string m) { var c = Console.ForegroundColor; Console.ForegroundColor = ConsoleColor.Yellow; Console.WriteLine(m); Console.ForegroundColor = c; }
        static void Error(string m) { var c = Console.ForegroundColor; Console.ForegroundColor = ConsoleColor.Red; Console.WriteLine(m); Console.ForegroundColor = c; }
    }
}

// --------------------------- Settings ---------------------------------------

internal sealed record Settings(string ScbPath, string Token, string Keyspace)
{
    /// Env first (ASTRA_SCB_PATH / ASTRA_TOKEN / ASTRA_KEYSPACE), then optional appsettings.json
    public static Settings Load()
    {
        var scb = Environment.GetEnvironmentVariable("ASTRA_SCB_PATH");
        var tok = Environment.GetEnvironmentVariable("ASTRA_TOKEN");
        var ksp = Environment.GetEnvironmentVariable("ASTRA_KEYSPACE");

        if (string.IsNullOrWhiteSpace(scb) || string.IsNullOrWhiteSpace(tok) || string.IsNullOrWhiteSpace(ksp))
        {
            var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
            if (File.Exists(path))
            {
                try
                {
                    using var s = File.OpenRead(path);
                    using var doc = JsonDocument.Parse(s);
                    if (doc.RootElement.TryGetProperty("Astra", out var astra))
                    {
                        scb ??= astra.TryGetProperty("ScbPath", out var v1) ? v1.GetString() : null;
                        tok ??= astra.TryGetProperty("Token", out var v2) ? v2.GetString() : null;
                        ksp ??= astra.TryGetProperty("Keyspace", out var v3) ? v3.GetString() : null;
                    }
                }
                catch { /* ignore */ }
            }
        }

        // local defaults (edit for dev)
        scb ??= @"C:\path\secure-connect-bundle.zip";
        tok ??= "AstraCS:replace-me";
        ksp ??= "dev_ks";

        return new Settings(scb, tok, ksp);
    }
}

// --------------------------- Domain -----------------------------------------

public sealed record User(Guid UserId, string Email, string Name, string Password);

// --------------------------- CRUD -------------------------------------------

public sealed class UserCrud : IAsyncDisposable
{
    private readonly ICluster _cluster;
    private readonly ISession _session;

    // main table (PK = user_id)
    private PreparedStatement _insUsers = null!;
    private PreparedStatement _selById = null!;
    private PreparedStatement _updNameById = null!;
    private PreparedStatement _delById = null!;

    // query table (PK = email, CK = user_id)
    private PreparedStatement _insUsersByEmail = null!;
    private PreparedStatement _selByEmail = null!;
    private PreparedStatement _updNameByEmail = null!;
    private PreparedStatement _delByEmailAndId = null!;

    private UserCrud(ICluster cluster, ISession session)
    {
        _cluster = cluster;
        _session = session;
    }

    public static async Task<UserCrud> ConnectAsync(string scbPath, string token, string keyspace, CancellationToken ct = default)
    {
        var cluster = Cluster.Builder()
            .WithCloudSecureConnectionBundle(scbPath)
            .WithCredentials("token", token)
            .Build();

        ct.ThrowIfCancellationRequested();
        var session = await cluster.ConnectAsync().ConfigureAwait(false);
        await session.ChangeKeyspaceAsync(keyspace).ConfigureAwait(false);

        var crud = new UserCrud(cluster, session);
        await crud.PrepareAsync();
        return crud;
    }

    private async Task PrepareAsync()
    {
        // base tables
        _insUsers = await _session.PrepareAsync("INSERT INTO users (user_id,email,name,password) VALUES (?,?,?,?)");
        _selById = await _session.PrepareAsync("SELECT user_id,email,name,password FROM users WHERE user_id = ?");
        _updNameById = await _session.PrepareAsync("UPDATE users SET name = ? WHERE user_id = ?");
        _delById = await _session.PrepareAsync("DELETE FROM users WHERE user_id = ?");

        // query table: users_by_email (email, user_id) as key
        _insUsersByEmail = await _session.PrepareAsync("INSERT INTO users_by_email (email,user_id,name,password) VALUES (?,?,?,?)");
        _selByEmail = await _session.PrepareAsync("SELECT email,user_id,name,password FROM users_by_email WHERE email = ?");
        _updNameByEmail = await _session.PrepareAsync("UPDATE users_by_email SET name = ? WHERE email = ? AND user_id = ?");
        _delByEmailAndId = await _session.PrepareAsync("DELETE FROM users_by_email WHERE email = ? AND user_id = ?");
    }

    /// Call once if needed; safe to run repeatedly.
    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        // Assumes keyspace already selected.
        var ddlUsers =
            "CREATE TABLE IF NOT EXISTS users (" +
            "  user_id uuid PRIMARY KEY," +
            "  email text, name text, password text" +
            ")";
        var ddlUsersByEmail =
            "CREATE TABLE IF NOT EXISTS users_by_email (" +
            "  email text," +
            "  user_id uuid," +
            "  name text, password text," +
            "  PRIMARY KEY (email, user_id)" +
            ")";
        await _session.ExecuteAsync(new SimpleStatement(ddlUsers)).ConfigureAwait(false);
        await _session.ExecuteAsync(new SimpleStatement(ddlUsersByEmail)).ConfigureAwait(false);
    }

    public async Task InsertAsync(User u, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await _session.ExecuteAsync(_insUsers.Bind(u.UserId, u.Email, u.Name, u.Password)).ConfigureAwait(false);
        await _session.ExecuteAsync(_insUsersByEmail.Bind(u.Email, u.UserId, u.Name, u.Password)).ConfigureAwait(false);
    }

    public async Task<User?> GetByIdAsync(Guid userId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var rs = await _session.ExecuteAsync(_selById.Bind(userId)).ConfigureAwait(false);
        var r = rs.FirstOrDefault();
        return r is null ? null : new User(
            r.GetValue<Guid>("user_id"),
            r.GetValue<string>("email"),
            r.GetValue<string>("name"),
            r.GetValue<string>("password"));
    }

    public async Task<User?> GetByEmailAsync(string email, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var rs = await _session.ExecuteAsync(_selByEmail.Bind(email)).ConfigureAwait(false);
        var r = rs.FirstOrDefault();
        return r is null ? null : new User(
            r.GetValue<Guid>("user_id"),
            r.GetValue<string>("email"),
            r.GetValue<string>("name"),
            r.GetValue<string>("password"));
    }

    public async Task UpdateNameAsync(Guid userId, string email, string newName, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await _session.ExecuteAsync(_updNameById.Bind(newName, userId)).ConfigureAwait(false);
        await _session.ExecuteAsync(_updNameByEmail.Bind(newName, email, userId)).ConfigureAwait(false);
    }

    public async Task DeleteByIdAsync(Guid userId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        // fetch email (anchored on PK) so we can delete from users_by_email safely
        var rs = await _session.ExecuteAsync(_selById.Bind(userId)).ConfigureAwait(false);
        var r = rs.FirstOrDefault();
        if (r is not null)
        {
            var email = r.GetValue<string>("email");
            await _session.ExecuteAsync(_delByEmailAndId.Bind(email, userId)).ConfigureAwait(false);
        }

        await _session.ExecuteAsync(_delById.Bind(userId)).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        _session?.Dispose();
        _cluster?.Dispose();
        return ValueTask.CompletedTask;
    }
}

// --------------------------- Table Printer ----------------------------------

internal static class Table
{
    public static void Print<T>(System.Collections.Generic.IEnumerable<T> rows, params (string Header, Func<T, object?> Selector)[] cols)
    {
        var data = rows?.ToList() ?? new System.Collections.Generic.List<T>();
        if (cols is null || cols.Length == 0) { Console.WriteLine("(no columns)"); return; }

        var widths = GetWidths(data, cols);
        Sep(widths);
        Console.WriteLine("| " + string.Join(" | ", cols.Select((c, i) => c.Header.PadRight(widths[i]))) + " |");
        Sep(widths);

        if (data.Count == 0) { Console.WriteLine("(no rows)"); Sep(widths); return; }

        foreach (var r in data)
        {
            var cells = cols.Select((c, i) => (c.Selector(r)?.ToString() ?? "").PadRight(widths[i]));
            Console.WriteLine("| " + string.Join(" | ", cells) + " |");
        }
        Sep(widths);

        static int[] GetWidths(System.Collections.Generic.IReadOnlyList<T> rowsLocal, (string Header, Func<T, object?> Selector)[] columns)
        {
            var w = new int[columns.Length];
            for (int i = 0; i < columns.Length; i++) w[i] = columns[i].Header.Length;
            foreach (var r in rowsLocal)
                for (int i = 0; i < columns.Length; i++)
                {
                    var s = columns[i].Selector(r)?.ToString() ?? "";
                    if (s.Length > w[i]) w[i] = s.Length;
                }
            return w;
        }
        static void Sep(int[] widths) => Console.WriteLine("+-" + string.Join("-+-", widths.Select(w => new string('-', w))) + "-+");
    }
}
