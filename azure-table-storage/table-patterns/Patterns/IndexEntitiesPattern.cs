using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AzureTableStorage.Patterns.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    internal class IndexEntitiesPattern : Pattern
    {
        public IndexEntitiesPattern(CloudTableClient tableClient, string tableName)
        {
            _tableClient = tableClient;
            _tableName = tableName;
        }

        public override async Task ExecuteAsync(CloudTable cloudTable = null)
        {
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
            await cloudTable.ExecuteBatchAsync(batchOperation1);
            await cloudTable.ExecuteBatchAsync(batchOperation2);
            await cloudTable.ExecuteBatchAsync(batchOperation3);

            // Insert Index Entities
            var index1 = new Index(partitionKey: "Comedy", rowKey: "2011") { IndexEntities = string.Join(',', moviesType1.ToArray()) };
            var index2 = new Index(partitionKey: "Horror", rowKey: "2001") { IndexEntities = string.Join(',', moviesType2.ToArray()) };

            TableOperation insert1 = TableOperation.Insert(index1);
            TableOperation insert2 = TableOperation.Insert(index2);

            // Execute insert operations
            Console.WriteLine("...Execute insert operations...");
            await cloudTable.ExecuteAsync(insert1);
            await cloudTable.ExecuteAsync(insert2);


            // Retrive index
            Console.WriteLine("...Retrive index...");
            TableQuery<Index> rangeQuery1 = new TableQuery<Index>()
                .Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Comedy")
                    , TableOperators.And
                    , TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "2011")));

            var result1 = await cloudTable.ExecuteQuerySegmentedAsync(rangeQuery1, null);
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

            var movies = await cloudTable.ExecuteQuerySegmentedAsync(query, null);
            foreach (var movie in movies)
            {
                Console.WriteLine($"Movie Type: {movie.Type}, Premiere: {movie.Premiere}, Title: {movie.Title}");
            }
        }
    }
}
