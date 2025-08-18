using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.DataApi
{
    public sealed class AstraDataApiClient : IDisposable
    {
        private readonly HttpClient _http;
        private readonly string _endpoint; // e.g. https://<db-id>-<region>.apps.astra.datastax.com

        public AstraDataApiClient(string token, string endpoint)
        {
            if (string.IsNullOrWhiteSpace(token)) throw new ArgumentException("Token is required", nameof(token));
            if (string.IsNullOrWhiteSpace(endpoint)) throw new ArgumentException("Endpoint is required", nameof(endpoint));

            _endpoint = endpoint.TrimEnd('/');
            _http = new HttpClient();
            // Data API uses the "Token" header (NOT Authorization / NOT X-Cassandra-Token)
            _http.DefaultRequestHeaders.Remove("Token");
            _http.DefaultRequestHeaders.Add("Token", token);
        }

        public Database GetDatabase(string keyspace) => new Database(_http, _endpoint, keyspace);

        public void Dispose() => _http.Dispose();
    }

    public sealed class Database
    {
        internal static readonly JsonSerializerOptions JsonOpts = new()
        {
            PropertyNameCaseInsensitive = true,  // important
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        private readonly HttpClient _http;
        private readonly string _base; // {endpoint}/api/json/v1/{keyspace}

        internal Database(HttpClient http, string endpoint, string keyspace)
        {
            if (string.IsNullOrWhiteSpace(keyspace)) throw new ArgumentException("Keyspace is required", nameof(keyspace));
            _http = http;
            _base = $"{endpoint}/api/json/v1/{keyspace}";
        }

        public Collection<T> GetCollection<T>(string collectionName) where T : class =>
            new Collection<T>(_http, $"{_base}/{collectionName}");
    }

    public sealed class Collection<TDocument> where TDocument : class
    {
        private readonly HttpClient _http;
        private readonly string _url; // {endpoint}/api/json/v1/{keyspace}/{collection}

        internal Collection(HttpClient http, string url)
        {
            _http = http;
            _url = url;
        }

        /// <summary>Insert a single document.</summary>
        public async Task<string?> InsertOneAsync<TDocument>(TDocument doc, CancellationToken ct = default)
            where TDocument : class
        {
            var payload = new { insertOne = new { document = doc } };
            var resp = await PostAsync(payload, ct);

            // Try strict DTO first if you want (optional), then fallback:
            return ExtractInsertedId(resp);
        }

        /// <summary>Find many documents (optionally with a filter object).</summary>

        public async Task<IReadOnlyList<TDocument>> FindAsync(object? filter = null, int? limit = null, CancellationToken ct = default)
        {
            var payload = new { find = new { filter, options = limit is null ? null : new { limit } } };
            var resp = await PostAsync(payload, ct);

            var env = JsonSerializer.Deserialize<FindEnvelope<TDocument>>(resp, Database.JsonOpts);
            return env?.Data?.Documents ?? [];
        }

        /// <summary>Find a single document by filter (returns null if not found).</summary>
        public async Task<TDocument?> FindOneAsync(object filter, CancellationToken ct = default)
        {
            var payload = new { findOne = new { filter } };
            var resp = await PostAsync(payload, ct);

            var env = Deserialize<FindOneEnvelope<TDocument>>(resp);
            return env?.Data?.Document;
        }

        /// <summary>Update a single document (Mongo-like filter + update).</summary>
        /// Example update: new { $set = new { name = "New Name" } }
        public async Task<UpdateOneResult> UpdateOneAsync(object filter, object update, bool upsert = false, CancellationToken ct = default)
        {
            var payload = new { updateOne = new { filter, update, options = new { upsert } } };
            var resp = await PostAsync(payload, ct);
            return Deserialize<UpdateOneResult>(resp);
        }

        /// <summary>Delete a single document matching the filter.</summary>
        public async Task<DeleteOneResult> DeleteOneAsync(object filter, CancellationToken ct = default)
        {
            var payload = new { deleteOne = new { filter } };
            var resp = await PostAsync(payload, ct);
            return Deserialize<DeleteOneResult>(resp);
        }

        // --- internals ---

        private async Task<string> PostAsync(object body, CancellationToken ct)
        {
            var json = JsonSerializer.Serialize(body, Database.JsonOpts);
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            using var resp = await _http.PostAsync(_url, content, ct);

            var respText = await resp.Content.ReadAsStringAsync(ct);
            if (!resp.IsSuccessStatusCode)
            {
                throw new HttpRequestException($"Data API error {(int)resp.StatusCode} {resp.StatusCode}: {respText}");
            }
            return respText;
        }

        private static TOut Deserialize<TOut>(string json) =>
            JsonSerializer.Deserialize<TOut>(json, Database.JsonOpts)!;

        private static string? ExtractInsertedId(string resp)
        {
            using var doc = JsonDocument.Parse(resp);
            var root = doc.RootElement;

            // status.insertedId (string)
            if (root.TryGetProperty("status", out var status))
            {
                if (status.TryGetProperty("insertedId", out var insertedId) &&
                    insertedId.ValueKind == JsonValueKind.String)
                {
                    return insertedId.GetString();
                }

                // status.insertedIds (array or array-of-arrays)
                if (status.TryGetProperty("insertedIds", out var insertedIds) &&
                    insertedIds.ValueKind == JsonValueKind.Array)
                {
                    // Case 1: ["id1", "id2", ...]
                    foreach (var item in insertedIds.EnumerateArray())
                    {
                        if (item.ValueKind == JsonValueKind.String)
                            return item.GetString();

                        // Case 2: [["id1"], ["id2"], ...]
                        if (item.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var inner in item.EnumerateArray())
                            {
                                if (inner.ValueKind == JsonValueKind.String)
                                    return inner.GetString();
                            }
                        }
                    }
                }
            }

            // data.documentId (string)
            if (root.TryGetProperty("data", out var data))
            {
                if (data.TryGetProperty("documentId", out var docId) &&
                    docId.ValueKind == JsonValueKind.String)
                {
                    return docId.GetString();
                }

                // data.documentIds (array)
                if (data.TryGetProperty("documentIds", out var docIds) &&
                    docIds.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in docIds.EnumerateArray())
                    {
                        if (item.ValueKind == JsonValueKind.String)
                            return item.GetString();
                    }
                }
            }

            return null; // not found
        }

    }

    // --- minimal response DTOs ---

    public sealed class InsertOneEnvelope
    {
        [JsonPropertyName("data")]
        public InsertOneData? Data { get; set; }

        [JsonPropertyName("status")]
        public InsertOneStatus? Status { get; set; }
    }

    public sealed class InsertOneData
    {
        // Some responses include the inserted document (or id) under data
        [JsonPropertyName("document")]
        public object? Document { get; set; }

        [JsonPropertyName("documentId")]
        public string? DocumentId { get; set; }
    }

    public sealed class InsertOneStatus
    {
        // Many Astra responses include "insertedId" here
        [JsonPropertyName("insertedId")]
        public string? InsertedId { get; set; }

        // Keep any extra fields without failing deserialization
        [JsonExtensionData]
        public Dictionary<string, System.Text.Json.JsonElement>? Extra { get; set; }
    }


    public sealed class FindResult<T> where T : class
    {
        [JsonPropertyName("find")]
        public FindData<T>? Find { get; set; }

        [JsonIgnore]
        public IReadOnlyList<T>? Data => Find?.Data;
    }

    public sealed class FindData<T>
    {
        [JsonPropertyName("data")]
        public IReadOnlyList<T>? Data { get; set; }
    }

    public sealed class FindOneResult<T> where T : class
    {
        public FindOneData<T>? FindOne { get; set; }
        public T? Data => FindOne?.Data;
    }


    public sealed class FindOneData<T> where T : class
    {
        [JsonPropertyName("data")]
        public T? Data { get; set; }
    }

    public sealed class FindEnvelope<T> where T : class
    {
        public FindDataNode<T>? Data { get; set; }
        public object? Status { get; set; }
    }

    public sealed class FindOneEnvelope<T> where T : class
    {
        [JsonPropertyName("data")]
        public FindOneDataNode<T>? Data { get; set; }

        [JsonPropertyName("status")]
        public object? Status { get; set; }
    }

    public sealed class FindOneDataNode<T> where T : class
    {
        [JsonPropertyName("document")]
        public T? Document { get; set; }
    }

    public sealed class FindDataNode<T> where T : class
    {
        public List<T>? Documents { get; set; }
        public string? PageState { get; set; }
    }

    public sealed class UpdateOneResult
    {
        [JsonPropertyName("updateOne")]
        public UpdateOneData? UpdateOne { get; set; }
    }

    public sealed class UpdateOneData
    {
        [JsonPropertyName("matchedCount")]
        public int MatchedCount { get; set; }
        [JsonPropertyName("modifiedCount")]
        public int ModifiedCount { get; set; }
        [JsonPropertyName("upsertedId")]
        public string? UpsertedId { get; set; }
    }

    public sealed class DeleteOneResult
    {
        [JsonPropertyName("deleteOne")]
        public DeleteOneData? DeleteOne { get; set; }
    }

    public sealed class DeleteOneData
    {
        [JsonPropertyName("deletedCount")]
        public int DeletedCount { get; set; }
    }
}
