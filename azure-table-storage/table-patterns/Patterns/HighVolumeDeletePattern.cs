using System;
using System.Linq;
using System.Threading.Tasks;
using AzureTableStorage.Patterns.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    internal class HighVolumeDeletePattern : Pattern
    {
        public HighVolumeDeletePattern(CloudTableClient tableClient, string tableName)
        {
            _tableClient = tableClient;
            _tableName = tableName;
        }
        public override async Task ExecuteAsync(CloudTable cloudTable = null)
        {
            string invertedTicks = string.Empty;
            TableBatchOperation insertOperation = new TableBatchOperation();

            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var addMovieToCart = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks, Event = "ADD_TO_CART", Title = "The Matrix" };
            insertOperation.Insert(addMovieToCart);

            await Task.Delay(2000);

            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var placeOrder = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks, Event = "PLACE_ORDER", Title = "The Matrix" };
            insertOperation.Insert(placeOrder);

            await Task.Delay(2000);

            invertedTicks = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
            var startWatching = new Order { PartitionKey = "CLIENT_test@test.pl", RowKey = invertedTicks, Event = "START_WATCHING", Title = "The Matrix" };
            insertOperation.Insert(startWatching);

            Console.WriteLine("...Execute batch insert...");
            await cloudTable.ExecuteBatchAsync(insertOperation);

            // Retrive data

            Console.WriteLine("...Retrive data...");

            TableBatchOperation removeOperation = new TableBatchOperation();
            TableQuery<Order> query = new TableQuery<Order>();
            query.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "CLIENT_test@test.pl"));
            var orders = await cloudTable.ExecuteQuerySegmentedAsync(query, null);
            foreach(var order in orders.OrderByDescending(o => o.RowKey))
            {
                DateTime datetime = new DateTime(DateTime.MaxValue.Ticks - Int64.Parse(order.RowKey));
                Console.WriteLine($"Order: {datetime}; Event type: {order.Event}; Item: {order.Title}");
                removeOperation.Delete(order);
            }

            
            Console.WriteLine("...Delete volume data...");
            
            await cloudTable.ExecuteBatchAsync(removeOperation);
        }
    }
}