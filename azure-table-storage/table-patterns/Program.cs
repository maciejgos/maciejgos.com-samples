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
        static CloudTable _table;

        static void Main(string[] args)
        {
            ReadConfiguration();
            Initialize();

            // Index entities pattern
            Console.WriteLine("Index entities pattern");
            CreateIndexEntitiesPatternDemo().Wait();

            Console.WriteLine("Log tail pattern");
            CreateLogTailPatternDemo().Wait();
        }

        private static async Task CreateLogTailPatternDemo()
        {
            _table = _tableClient.GetTableReference("orders");

            // Create table if not exists
            Console.WriteLine("...Create table if not exists...");
            await _table.CreateIfNotExistsAsync();

            string invertedTicks = string.Empty;
            TableBatchOperation insertOperation = new TableBatchOperation();
            // User add movie to cart.
            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var matrixAddToCart = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks,  Event = "ADD_TO_CART", Title = "The Matrix" };
            insertOperation.Insert(matrixAddToCart);

            // User change his mind and remove movie from cart.
            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var removeFromCart = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks,  Event = "REMOVE_FROM_CART", Title = "The Matrix" };
            insertOperation.Insert(removeFromCart);

            // User add movie to cart.
            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var horribleBossesaddToCart = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks,  Event = "ADD_TO_CART", Title = "Horrible Bosses" };
            insertOperation.Insert(horribleBossesaddToCart);

            // User add provide card details.
            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var creditCardDetails = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks,  Event = "ADD_CARD_DETAILS", Title = "Horrible Bosses" };
            insertOperation.Insert(creditCardDetails);

            // User add provide card details.
            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var placeOrder = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks,  Event = "PLACE_ORDER", Title = "Horrible Bosses" };
            insertOperation.Insert(placeOrder);

            // User start watching a movie.
            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var startWatching = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks,  Event = "START_WATCHING", Title = "Horrible Bosses" };
            insertOperation.Insert(startWatching);

            // Execute batch operation
            Console.WriteLine("...Execute batch operation...");
            await _table.ExecuteBatchAsync(insertOperation);

            // Retrive data
            TableQuery<Order> query = new TableQuery<Order>();
            query.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "CLIENT_test@test.pl"));
            var orders = await _table.ExecuteQuerySegmentedAsync(query, null);
            foreach(var order in orders.OrderBy(o => o.ETag))
            {
                DateTime datetime = new DateTime(DateTime.MaxValue.Ticks - Int64.Parse(order.RowKey));
                Console.WriteLine($"Order: {datetime}; Event type: {order.Event}; Item: {order.Title}");
            }

            // Remove data / Cleanup
            Console.WriteLine("...Cleanup...");
            await _table.DeleteIfExistsAsync();
            _table = null;
        }

        static async Task CreateIndexEntitiesPatternDemo()
        {
            _table = _tableClient.GetTableReference("movies");

            // Create table if not exists
            Console.WriteLine("...Create table if not exists...");
            await _table.CreateIfNotExistsAsync();

            // Collection of ids
            List<string> moviesType1 = new List<string>();
            List<string> moviesType2 = new List<string>();

            // Prepare batch operation
            TableBatchOperation batchOperation1 = new TableBatchOperation();
            TableBatchOperation batchOperation2 = new TableBatchOperation();
            TableBatchOperation batchOperation3 = new TableBatchOperation();

            // Generate some data
            Console.WriteLine("...Generate test data...");
            for (int i = 1; i <= 10; i++)
            {
                var movie = new Movie(type: "Comedy", title: $"Movie {i}")
                {
                    Id = i,
                    Title = $"Movie {i}",
                    Description = "Sample description",
                    Type = "Comedy",
                    Premiere = 2011,
                    Timestamp = new DateTimeOffset(DateTime.UtcNow)
                };

                moviesType1.Add(movie.Title);
                batchOperation1.Insert(movie);
            }

            // Generate some data
            for (int i = 1; i <= 10; i++)
            {
                var movie = new Movie(type: "Horror", title: $"Other Movie {i}")
                {
                    Id = i,
                    Title = $"Other Movie {i}",
                    Description = "Sample description",
                    Type = "Horror",
                    Premiere = 2001,
                    Timestamp = new DateTimeOffset(DateTime.UtcNow)
                };

                moviesType2.Add(movie.Title);
                batchOperation2.Insert(movie);
            }

            for (int i = 1; i <= 10; i++)
            {
                var movie = new Movie(type: "Horror", title: $"Other Horror Movie {i}")
                {
                    Id = i,
                    Title = $"Other Horror Movie {i}",
                    Description = "Sample description",
                    Type = "Horror",
                    Premiere = 2002,
                    Timestamp = new DateTimeOffset(DateTime.UtcNow)
                };

                batchOperation3.Insert(movie);
            }

            // Execute batch
            Console.WriteLine("...Execute batch operations...");
            await _table.ExecuteBatchAsync(batchOperation1);
            await _table.ExecuteBatchAsync(batchOperation2);
            await _table.ExecuteBatchAsync(batchOperation3);

            // Insert Index Entities
            var index1 = new Index(partitionKey: "Comedy", rowKey: "2011") { IndexEntities = string.Join(',', moviesType1.ToArray()) };
            var index2 = new Index(partitionKey: "Horror", rowKey: "2001") { IndexEntities = string.Join(',', moviesType2.ToArray()) };

            TableOperation insert1 = TableOperation.Insert(index1);
            TableOperation insert2 = TableOperation.Insert(index2);

            // Execute insert operations
            Console.WriteLine("...Execute insert operations...");
            await _table.ExecuteAsync(insert1);
            await _table.ExecuteAsync(insert2);


            // Retrive index
            Console.WriteLine("...Retrive index...");
            TableQuery<Index> rangeQuery1 = new TableQuery<Index>()
                .Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Comedy")
                    , TableOperators.And
                    , TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "2011")));

            var result1 = await _table.ExecuteQuerySegmentedAsync(rangeQuery1, null);
            string[] movieTitles = { };
            foreach (var index in result1)
            {
                Console.WriteLine($"{nameof(index.PartitionKey)}: {index.PartitionKey}, {nameof(index.RowKey)}: {index.RowKey}, {nameof(index.IndexEntities)}: {index.IndexEntities}");
                movieTitles = index.IndexEntities.Split(',');
            }

            // Retrive entities
            Console.WriteLine("...Retrive entities - using index...");
            TableQuery<Movie> query = new TableQuery<Movie>();

            query.Where(
                TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Comedy"),
                TableOperators.And,
                TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, "Movie 1"),
                TableOperators.Or,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, "Movie 10")))
            );

            var movies = await _table.ExecuteQuerySegmentedAsync(query, null);
            foreach (var movie in movies)
            {
                Console.WriteLine($"Movie Type: {movie.Type}, Premiere: {movie.Premiere}, Title: {movie.Title}");
            }

            // Remove data / Cleanup
            Console.WriteLine("...Cleanup...");
            await _table.DeleteIfExistsAsync();
            _table = null;
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
