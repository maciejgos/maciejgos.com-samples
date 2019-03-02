using System;
using System.Linq;
using System.Threading.Tasks;
using AzureTableStorage.Patterns.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    internal class LogTailPattern : Pattern
    {
        public LogTailPattern(CloudTableClient tableClient, string tableName)
        {
            _tableClient = tableClient;
            _tableName = tableName;
        }

        public override async Task ExecuteAsync(CloudTable cloudTable = null)
        {
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
            await cloudTable.ExecuteBatchAsync(insertOperation);

            // Retrive data
            TableQuery<Order> query = new TableQuery<Order>();
            query.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "CLIENT_test@test.pl"));
            var orders = await cloudTable.ExecuteQuerySegmentedAsync(query, null);
            foreach(var order in orders.OrderBy(o => o.ETag))
            {
                DateTime datetime = new DateTime(DateTime.MaxValue.Ticks - Int64.Parse(order.RowKey));
                Console.WriteLine($"Order: {datetime}; Event type: {order.Event}; Item: {order.Title}");
            }
        }
    }
}