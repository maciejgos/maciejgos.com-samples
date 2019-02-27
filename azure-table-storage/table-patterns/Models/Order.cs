using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns.Models
{
    public class Order : TableEntity
    {
        public string Event { get; set; }
        public string Title { get; set; }
    }
}