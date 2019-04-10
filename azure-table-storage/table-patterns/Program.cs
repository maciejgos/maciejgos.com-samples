using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    internal static class Program
    {
        static string _connectionString;
        static CloudStorageAccount _storageAccount;
        static CloudTableClient _tableClient;
        static CloudBlobClient _blobClient;

        static async Task Main(string[] args)
        {
            ReadConfiguration();
            Initialize();

            /*
                0 - run all
                1 - index entities pattern
                2 - log tail pattern
                3 - data series pattern
                4 - wide entities pattern
                5 - large entities pattern
                6 - High volume delete pattern
             */
            
            Console.WriteLine("Please select:" +
                              "\n0 - run all" +
                              "\n1 - index entities pattern" +
                              "\n2 - log tail pattern" +
                              "\n3 - data series pattern" +
                              "\n4 - wide entities pattern" +
                              "\n5 - large entities pattern" +
                              "\n6 - High volume delete pattern");

            var input = string.Empty;

            while (string.IsNullOrWhiteSpace(input))
            {
                input = Console.ReadLine();
            }

            while (true)
            {                
                if (int.TryParse(input, out var option)) return;
            
                switch (option)
                {
                    case 0:
                        await RunAll();
                        break;
                    case 1:
                        await RunIndexEntitiesPattern();
                        break;
                    case 2:
                        await RunLogTailPattern();
                        break;
                    case 3:
                        await RunDataSeriesPattern();
                        break;
                    case 4:
                        await RunWideEntitiesPattern();
                        break;
                    case 5:
                        await RunLargeEntitiesPattern();
                        break;
                    case 6:
                        await RunHighVolumeDeletePattern();
                        break;
                    default:
                        Console.WriteLine("Please select value in range 0-6");
                }
            }
        }

        private static async Task RunHighVolumeDeletePattern()
        {
            var highVolumeDeleteContext = new Context(new HighVolumeDeletePattern(_tableClient, $"orders{DateTime.UtcNow:yyyyMMdd}"));
            await highVolumeDeleteContext.RunAsync();
        }

        private static async Task RunLargeEntitiesPattern()
        {
            var largeEntitiesContext = new Context(new LargeEntitiesPattern(_tableClient, _blobClient, "largeEntities"));
            await largeEntitiesContext.RunAsync();
        }

        private static async Task RunWideEntitiesPattern()
        {
            var wideEntitiesContext = new Context(new WideEntitiesPattern( _tableClient, "wideEntities"));
            await wideEntitiesContext.RunAsync();
        }

        private static async Task RunDataSeriesPattern()
        {
            var dataSeriesContext = new Context(new DataSeriesPattern(_tableClient, "ordersStats"));
            await dataSeriesContext.RunAsync();
        }

        private static async Task RunLogTailPattern()
        {
            var logTailContext = new Context(new LogTailPattern(_tableClient, "orders"));
            await logTailContext.RunAsync();
        }

        private static async Task RunIndexEntitiesPattern()
        {
            Console.WriteLine("Start execution index entities pattern");
            
            var indexEntitiesContext = new Context(new IndexEntitiesPattern(_tableClient, "movies"));
            await indexEntitiesContext.RunAsync();
            
            Console.WriteLine("End execution index entities pattern");
        }

        private static async Task RunAll()
        {
            await RunIndexEntitiesPattern();
            await RunLogTailPattern();
            await RunDataSeriesPattern();
            await RunWideEntitiesPattern();
            await RunLargeEntitiesPattern();
            await RunHighVolumeDeletePattern();
        }

        private static void Initialize()
        {
            _storageAccount = CloudStorageAccount.Parse(_connectionString);
            _tableClient = _storageAccount.CreateCloudTableClient();
            _blobClient = _storageAccount.CreateCloudBlobClient();
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
