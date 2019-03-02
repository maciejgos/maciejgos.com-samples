using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    public abstract class Pattern
    {
        protected CloudTableClient _tableClient;
        protected string _tableName;

        public CloudTableClient TableClient => _tableClient;
        public string TableName => _tableName;

        public abstract Task ExecuteAsync(CloudTable cloudTable = null);
    }
}