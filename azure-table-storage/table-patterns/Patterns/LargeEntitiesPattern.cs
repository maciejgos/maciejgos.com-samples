using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorage.Patterns
{
    public class LargeEntitiesPattern : Pattern
    {
        private readonly CloudBlobClient _blobClient;
        public LargeEntitiesPattern(CloudTableClient tableClient, CloudBlobClient blobClient, string tableName)
        {
            _tableClient = tableClient;
            _blobClient = blobClient;
            _tableName = tableName;
        }

        public override async Task ExecuteAsync(CloudTable cloudTable = null)
        {
            // ...Upload file to blob...
            var blobContainer = _blobClient.GetContainerReference($"files_{Guid.NewGuid().ToString()}");
            await blobContainer.CreateIfNotExistsAsync();

            Console.WriteLine($"...Created container: {blobContainer.Name}...");

            var permissions = new BlobContainerPermissions
            {
                PublicAccess = BlobContainerPublicAccessType.Blob
            };

            await blobContainer.SetPermissionsAsync(permissions);
            var filepath = Path.Combine(Directory.GetCurrentDirectory(), "cover.jpg");
            var fileInfo = new FileInfo(filepath);

            Console.WriteLine("...Upload file...");

            var cloudBlob = blobContainer.GetBlockBlobReference(fileInfo.Name + Guid.NewGuid());
            await cloudBlob.UploadFromFileAsync(filepath);

            Console.WriteLine($"...File URI: {cloudBlob.Uri}");
        }
    }
}