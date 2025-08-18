# Cassandra .NET Examples

This repository contains practical examples showing how to connect a .NET application to [Apache Cassandra®](https://cassandra.apache.org/) and [DataStax Astra DB](https://www.datastax.com/astra), using both:

- **CQL Driver + Secure Connect Bundle (SCB)** — the low-level, high-performance way to query Cassandra
- **Data API (REST)** — a simple HTTP/JSON API for quick integrations

The goal is to provide **copy-paste friendly samples** you can adapt for real projects, while following best practices like prepared statements, paging, token authentication, and clean code structure.

---

## 📂 Repository Structure

cassandra-dotnet-examples/
├─ src/
│ ├─ Acme.Cassandra.SecureConnect/ # library wrapping driver + SCB
│ ├─ Acme.Cassandra.DataApi/ # simple Data API client helpers
├─ samples/
│ ├─ Samples.Console.Driver/ # SCB + token auth with CQL driver
│ ├─ Samples.Console.DataApi/ # REST Data API usage via HttpClient
├─ tests/
│ ├─ Acme.Cassandra.SecureConnect.Tests/
│ ├─ Acme.Cassandra.DataApi.Tests/

---

## 🚀 Getting Started

### Prerequisites
- [.NET 8 SDK](https://dotnet.microsoft.com/download)
- A [free Astra DB account](https://www.datastax.com/astra)
- A **Secure Connect Bundle (SCB)** for your database (download from Astra console)
- An **application token** (looks like `AstraCS:...`)

### Clone the repo
```bash
git clone https://github.com/your-org/cassandra-dotnet-examples.git
cd cassandra-dotnet-examples
```

Run SCB + Driver sample

Update samples/Samples.Console.Driver/appsettings.json with:

SecureConnectBundlePath → path to your downloaded SCB .zip

Token → your Astra token

Keyspace → your keyspace name

Run:
```bash
dotnet run --project samples/Samples.Console.Driver
```

Run Data API sample

Update samples/Samples.Console.DataApi/appsettings.json with:

Endpoint → your Astra REST API URL
(e.g. https://<db-id>-<region>.apps.astra.datastax.com/api/rest/v2/keyspaces/<keyspace>/users)

Token → your Astra token

Run:
```bash
dotnet run --project samples/Samples.Console.DataApi
```
🛠 Features Covered

✅ Connecting with Secure Connect Bundle (SCB) + token auth

✅ Safe queries with prepared statements (no injection risk)

✅ Paging large result sets with SetPageSize

✅ Insert & select posts/users from Cassandra tables

✅ Data API examples with HttpClient (CRUD over REST)

✅ Clean repo layout with samples, library code, and tests

📖 Learn More

Astra DB documentation

Cassandra .NET Driver

🤝 Contributing

Feel free to open issues or PRs if you’d like to improve the samples or add new scenarios.
