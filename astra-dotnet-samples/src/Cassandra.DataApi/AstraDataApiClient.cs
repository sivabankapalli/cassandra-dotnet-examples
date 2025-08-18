using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cassandra.DataApi
{
    public class AstraDataApiClient
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseEndpoint; // e.g. https://<dbid>-<region>.apps.../api/rest/v2/keyspaces/dev_cdk_ks

        public AstraDataApiClient(string endpoint, string token)
        {
            _baseEndpoint = endpoint.TrimEnd('/');
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Remove("X-Cassandra-Token");
            _httpClient.DefaultRequestHeaders.Add("X-Cassandra-Token", token); // <- key change
        }

        public async Task<string> GetAllRowsAsync(string table)
        {
            var url = $"{_baseEndpoint}/tables/{table}/rows";
            var resp = await _httpClient.GetAsync(url);
            resp.EnsureSuccessStatusCode();
            return await resp.Content.ReadAsStringAsync();
        }

        public async Task<string> InsertRowAsync<T>(string table, T row)
        {
            var url = $"{_baseEndpoint}/tables/{table}/rows";
            var json = JsonSerializer.Serialize(row);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var resp = await _httpClient.PostAsync(url, content);
            resp.EnsureSuccessStatusCode();
            return await resp.Content.ReadAsStringAsync();
        }
    }

}
