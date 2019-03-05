using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns.Models
{
    public class OrderStats : TableEntity
    {
        public int Count { get; set; }
    }
}