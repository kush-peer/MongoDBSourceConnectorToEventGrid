using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoSourceConnectorToEventGrid.EventGridPublisher;
using MongoSourceConnectorToEventGrid.Models;

namespace MongoSourceConnectorToEventGrid
{
    public class MongoDBChangeStreamService
    {
        protected readonly IMongoClient client;
        protected readonly IMongoDatabase database;
        private readonly IMongoCollection<BsonDocument> collection;
        private readonly IAppLogger<MongoDBChangeStreamService> logger;
        private readonly SemaphoreSlim semaphoreSlim = new(1, 1);
        EventGridPublisherService eventGridPublisherService;
        private BlobServiceClient blobServiceClient;
        private string container;
        private string storageAccountCon;
        private string dLGen2AccountName;
        private string dLGen2AccountKey;
        private string fileSystemName;
        private string dataLakeGen2Uri;

        #region Public Methods
        public MongoDBChangeStreamService(IMongoClient client, IAppLogger<MongoDBChangeStreamService> logger, 
            EventGridPublisherService eventGridPublisherService,  IConfiguration configuration)
        {
            this.database = client.GetDatabase(configuration["mongodb-database"]);
            this.collection = this.database.GetCollection<BsonDocument>(configuration["mongodb-collection"]);
            this.eventGridPublisherService = eventGridPublisherService;
            this.client = client;
            this.logger = logger;
            this.storageAccountCon = configuration["storage-account"];
            this.dLGen2AccountName = configuration["dataLakeGen2-accountName"];
            this.dLGen2AccountKey = configuration["dataLakeGen2-accountKey"];
            this.fileSystemName = configuration["fileSystemName"];
            this.dataLakeGen2Uri = configuration["dataLakeGen2Uri"];
            this.container = configuration["container"];
           
        }
        public void Init()
        {
            new Thread(async () => await ObserveCollections()).Start();
        }
        #endregion

        #region Private Methods
        private async Task ObserveCollections()
        {
            // Filter definition for document updated 
            var pipelineFilterDefinition = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                .Match(x => x.OperationType == ChangeStreamOperationType.Update
                || x.OperationType == ChangeStreamOperationType.Insert
                || x.OperationType == ChangeStreamOperationType.Delete);

            // choose stream option and set data lookup for full document 
            var changeStreamOptions = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
            };

            // Watches changes on the collection , no need to user cancellation token, mongo sdk already using it
            using var cursor = await this.collection.WatchAsync(pipelineFilterDefinition, changeStreamOptions);

            await this.semaphoreSlim.WaitAsync();

            // Run watch updated operations on returned cursor  from watch async
            await this.WatchCollectionUpdates(cursor);

            // release thread
            this.semaphoreSlim.Release();
        }
        private async Task WatchCollectionUpdates(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor)
        {
            await cursor?.ForEachAsync(async change =>
            {
                // If change is null - log information as null and return message
                if (change == null)
                {
                    this.logger.LogInformation("No changes tracked  by change stream  watcher");
                    return;
                }
                try
                {
                    // Deserialize full document with Plain object 
                    var updatedDocument = BsonSerializer.Deserialize<Dictionary<string, object>>(change.FullDocument);
                    
                    // remove _id aka objectId key from Mongodb
                    updatedDocument.Remove("_id");
                    
                    // Create event data object
                    var eventDetails = new EventDetails()
                    {
                        EventType = change.OperationType.ToString(),
                        Data = updatedDocument.ToJson(),
                        EventTime = DateTime.UtcNow,
                        Subject = "MongoDB Change Stream Connector",
                        Version = "1.0"
                    };

                    // Push info to Event Grid
                    // var isEventGridUpdated = await eventGridPublisherService.EventGridPublisher(eventDetails);

                    var isBlobUpdate = await UpdateStorage(updatedDocument);

                    // log information
                    if(isBlobUpdate) 
                        this.logger.LogInformation($"Changes tracked successfully by change stream : {eventDetails.Data}");
                    else
                    {
                        this.logger.LogError($"Unable to push changes to blob for type : {eventDetails.EventType}");
                    }
                }
                catch (MongoException exception)
                {
                    // log mongo exception - helpful for developers
                    this.logger.LogError("Change Stream watcher. Exception:" + exception.Message);
                }
            })!;
        }

        private async Task<bool> UpdateBlobStorage(IDictionary<string, object> updatedDocument)
        {
            try
            {
                
                // TODO Move configuration to service registrations
                
                var filePath = $"{container}-" + Guid.NewGuid() + ".json";
                this.blobServiceClient = new BlobServiceClient(storageAccountCon);
                var containerClient = this.blobServiceClient.GetBlobContainerClient(container);
                var blobClient = containerClient.GetBlobClient(filePath);
                // case 1 for json type
                await using var ms = new MemoryStream(Encoding.UTF8.GetBytes(updatedDocument.ToJson()));
                var blob = await blobClient.UploadAsync(ms);
                // case2 for avro types
                //var serializedContent = AvroConvert.Serialize(updatedDocument);
                //await blobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
                //var blob  =await blobClient.UploadAsync(new BinaryData(serializedContent));

                var uploadedVersion = blob.Value.VersionId != null;
                return uploadedVersion;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private async Task<bool> UpdateStorage(Dictionary<string, object> updatedDocument)
        {
            try
            {

                var filePath = $"{container}" + ".csv";

                StorageSharedKeyCredential sharedKeyCredential =
                     new(dLGen2AccountName, dLGen2AccountKey);
                DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient
                    (new Uri(dataLakeGen2Uri), sharedKeyCredential);
              
                DataLakeFileSystemClient fileSystemClient =
                  dataLakeServiceClient.GetFileSystemClient(fileSystemName);

                DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(container);

               
                DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(filePath);

                var csvFileFormat = String.Join(Environment.NewLine, updatedDocument.Select(d => $"{d.Key};{d.Value}"));
                
                // case 1 for json type to upload file
                await using var ms = new MemoryStream(Encoding.UTF8.GetBytes(csvFileFormat));
                await fileClient.DeleteIfExistsAsync();
                var file = await fileClient.UploadAsync(ms);
                var uploadedVer = file.Value.ToJson() != null;

                
                // case to append data
                //fileClient.CreateIfNotExists();
                //string leaseId = null;
                //long currentlength = fileClient.GetPropertiesAsync().Result.Value.ContentLength;
                //long fileSize = ms.Length;
                //var file = await fileClient.AppendAsync(ms, currentlength, leaseId:leaseId);
                //await fileClient.FlushAsync(position: currentlength + fileSize, close: true, conditions: new Azure.Storage.Files.DataLake.Models.DataLakeRequestConditions() { LeaseId = leaseId });
                ////ms.Position = 0;
                ////File.WriteAllBytes(filePath, ms.ToArray());
                //var uploadedVer = file.ToJson() != null;
                return uploadedVer;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        #endregion
    }
}
