using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    public class WideEntitiesPattern : Pattern
    {
        public WideEntitiesPattern(CloudTableClient tableClient, string tableName)
        {
            _tableClient = tableClient;
            _tableName = tableName;
        }

        public override async Task ExecuteAsync(CloudTable cloudTable = null)
        {
            Console.WriteLine("...Create big object...");
            var bigFakeDto = new BigFakeDto();
            var dataModelPart1 = DataModel1.CreateNew(bigFakeDto);
            var dataModelPart2 = DataModel2.CreateNew(bigFakeDto);

            TableBatchOperation insertOperation = new TableBatchOperation();
            insertOperation.Insert(dataModelPart1);
            insertOperation.Insert(dataModelPart2);

            Console.WriteLine("...Insert fake object into Table Storage...");

            await cloudTable.ExecuteBatchAsync(insertOperation);

            Console.WriteLine("...Retrive fake object...");

            var data1 = await cloudTable.ExecuteAsync(TableOperation.Retrieve<DataModel1>("bigFakeObject", "bigFakeObjectChunk1"));
            var data2 = await cloudTable.ExecuteAsync(TableOperation.Retrieve<DataModel2>("bigFakeObject", "bigFakeObjectChunk2"));

            var retrivedFakeObject = bigFakeDto.CreateNew(data1.Result, data2.Result);

            Console.WriteLine($"...Retrived fake object:\n\tProperty1: {retrivedFakeObject.Property1}\n\tProperty2: {retrivedFakeObject.Property2}\n\tProperty255: {retrivedFakeObject.Property255}");
        }
    }

    public class BigFakeDto
    {
        public string Property1 { get; set; }
        public string Property2 { get; set; }
        public string Property3 { get; set; }

        // Here we have some more properties...

        public string Property255 { get; set; }

        public BigFakeDto()
        {
            Property1 = "test 1";
            Property2 = "test 2";
            Property3 = "test 3";
            Property255 = "test255";
        }

        internal BigFakeDto CreateNew(object result1, object result2)
        {
            var data1 = result1 as DataModel1;
            var data2 = result2 as DataModel2;

            return new BigFakeDto
            {
                Property1 = data1.Property1,
                Property2 = data1.Property2,
                Property3 = data1.Property3,
                Property255 = data2.Property255
            };
        }
    }    

    public class DataModel1 : TableEntity
    {
        public string Property1 { get; set; }
        public string Property2 { get; set; }
        public string Property3 { get; set; }

        internal static DataModel1 CreateNew(BigFakeDto bigFakeDto)
        {
            return new DataModel1
            {
                PartitionKey = "bigFakeObject",
                RowKey = "bigFakeObjectChunk1",
                Property1 = bigFakeDto.Property1,
                Property2 = bigFakeDto.Property2,
                Property3 = bigFakeDto.Property3
            };
        }
    }

    public class DataModel2 : TableEntity
    {
        public string Property255 { get; set; }

        internal static DataModel2 CreateNew(BigFakeDto bigFakeDto)
        {
            return new DataModel2
            {
                PartitionKey = "bigFakeObject",
                RowKey = "bigFakeObjectChunk2",
                Property255 = bigFakeDto.Property255
            };
        }
    }
}