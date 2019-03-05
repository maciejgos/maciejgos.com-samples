using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AzureTableStorage.Patterns.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    class Program
    {
        static string _connectionString;
        static CloudStorageAccount _storageAccount;
        static CloudTableClient _tableClient;

        static void Main(string[] args)
        {
            ReadConfiguration();
            Initialize();

            var indexEntitiesContext = new Context(new IndexEntitiesPattern(tableClient: _tableClient, tableName: "movies"));
            indexEntitiesContext.RunAsync().Wait();

            var logTailContext = new Context(new LogTailPattern(tableClient: _tableClient, tableName: "orders"));
            logTailContext.RunAsync().Wait();

            var highVolumeDeleteContext = new Context(new HighVolumeDeletePattern(tableClient: _tableClient, tableName: $"orders{DateTime.UtcNow.ToString("yyyyMMdd")}"));
            highVolumeDeleteContext.RunAsync().Wait();
        }

        private static void Initialize()
        {
            _storageAccount = CloudStorageAccount.Parse(_connectionString);
            _tableClient = _storageAccount.CreateCloudTableClient();
        }

        static void ReadConfiguration()
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .Build();

            _connectionString = config["connectionString"];

            Debug.WriteLine($"Read param: [Connection String] - [Value]: {_connectionString}");
        }
    }
}
