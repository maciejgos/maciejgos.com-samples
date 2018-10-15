using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns.Models
{
    public class Movie : TableEntity
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Description { get; set; }
        public string Type { get; set; }
        public int Premiere { get; set; }

        public Movie(){}
        public Movie(string type, string title)
        {
            PartitionKey = type;
            RowKey = title;
        }
    }
}