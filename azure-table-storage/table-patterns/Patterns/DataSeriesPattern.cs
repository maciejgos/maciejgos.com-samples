using System;
using System.Threading.Tasks;
using AzureTableStorage.Patterns.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    public class DataSeriesPattern : Pattern
    {
        public DataSeriesPattern(CloudTableClient tableClient, string tableName)
        {
            _tableClient = tableClient;
            _tableName = tableName;
        }
        public override async Task ExecuteAsync(CloudTable cloudTable = null)
        {
            // Store data for analytics about amount each type of events in each hour.

            Console.WriteLine("...Generate some sample data...");

            var createOrderStats1 = new OrderStats
            {
                PartitionKey = "CREATE_ORDER",
                RowKey = DateTime.Now.Hour.ToString(),
                Count = 10
            };

            var createOrderStats2 = new OrderStats
            {
                PartitionKey = "CREATE_ORDER",
                RowKey = DateTime.Now.AddHours(1).ToString(),
                Count = 10
            };

            var startWatchingOrderStats1 = new OrderStats
            {
                PartitionKey = "START_WATCHING",
                RowKey = DateTime.Now.Hour.ToString(),
                Count = 8
            };

            var startWatchingOrderStats2 = new OrderStats
            {
                PartitionKey = "START_WATCHING",
                RowKey = DateTime.Now.AddHours(2).ToString(),
                Count = 8
            };            

            await cloudTable.ExecuteAsync(TableOperation.Insert(createOrderStats1));
            await cloudTable.ExecuteAsync(TableOperation.Insert(createOrderStats2));
            await cloudTable.ExecuteAsync(TableOperation.Insert(startWatchingOrderStats1));
            await cloudTable.ExecuteAsync(TableOperation.Insert(startWatchingOrderStats2));

            Console.WriteLine("...Fetch data for partition...");

            TableQuery<OrderStats> query1 = new TableQuery<OrderStats>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "CREATE_ORDER"));
            TableQuery<OrderStats> query2 = new TableQuery<OrderStats>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "START_WATCHING"));

            var data1 = await cloudTable.ExecuteQuerySegmentedAsync(query1, null);
            data1.Results.ForEach(os => Console.WriteLine($"Stats for \nEvent type: {os.PartitionKey}\n\tHour: {os.RowKey}\n\tCount: {os.Count}"));

            var data2 = await cloudTable.ExecuteQuerySegmentedAsync(query2, null);
            data2.Results.ForEach(os => Console.WriteLine($"Stats for \nEvent type: {os.PartitionKey}\n\tHour: {os.RowKey}\n\tCount: {os.Count}"));            
        }
    }
}