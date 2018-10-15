using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns.Models
{
    public class Index : TableEntity
    {
        public string IndexEntities { get; set; }

        public Index() {}
        public Index(string partitionKey, string rowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
        }
    }
}