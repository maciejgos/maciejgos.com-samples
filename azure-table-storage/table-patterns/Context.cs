using System;
using System.Threading.Tasks;
using AzureTableStorage.Patterns;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage
{
    public class Context
    {
        private Pattern _pattern;
        private CloudTableClient _tableClient;
        private CloudTable _cloudTable;
        private string _tableName;

        public Context(Pattern pattern)
        {
            _pattern = pattern;
            _tableClient = pattern.TableClient;
            _tableName = pattern.TableName;            
        }

        internal async Task RunAsync()
        {
            Console.WriteLine($"...Create table {_tableName} if not exists...");

            await CreateTableAsync();

            Console.WriteLine("...Execute pattern logic...");

            await _pattern.ExecuteAsync(_cloudTable);

            Console.WriteLine("...Cleanup...");

            await DeleteTableAsync();
        }

        private async Task CreateTableAsync()
        {
            _cloudTable = _tableClient.GetTableReference(_tableName);

            await _cloudTable.CreateIfNotExistsAsync();            
        }

        private async Task DeleteTableAsync()
        {
            await _cloudTable.DeleteIfExistsAsync();
        }
    }
}