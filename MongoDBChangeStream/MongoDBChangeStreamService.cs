using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
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

        /// <summary>
        /// Intiliaze Thread
        /// </summary>
        public void Init()
        {
            new Thread(async () => await ObserveCollections()).Start();
        }
        #endregion

        #region Private Methods

        /// <summary>
        /// Observe Collection for Update, insert or Delete activities 
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Mongo DB change stream to track changes on collection
        /// </summary>
        /// <param name="cursor"></param>
        /// <returns></returns>

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
                        //Data = updatedDocument.ToJson(), this is actual delta data coming from Mongodb
                        EventTime = DateTime.UtcNow,
                        Subject = "MongoDB Change Stream Connector",
                        Version = "1.0"
                    };

                    // In case of custom events, use thos code
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
                    // log mongodb exception - helpful for developers
                    this.logger.LogError("Change Stream watcher. Exception:" + exception.Message);
                }
            })!;
        }

        /// <summary>
        /// Upload delta changes to ADL gen 2
        /// </summary>
        /// <param name="updatedDocument"></param>
        /// <returns></returns>
        private async Task<bool> UpdateStorage(Dictionary<string, object> updatedDocument)
        {
            try
            {

                StorageSharedKeyCredential sharedKeyCredential =  new(dLGen2AccountName, dLGen2AccountKey);
                DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient (new Uri(dataLakeGen2Uri), sharedKeyCredential);
              
                DataLakeFileSystemClient fileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);

                DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(container);
                
                // json file type 
                var filePath = $"{container}" + ".json";
                DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(filePath);
                await using var ms = new MemoryStream(Encoding.UTF8.GetBytes(updatedDocument.ToJson()));

                await fileClient.DeleteIfExistsAsync();
                var file = await fileClient.UploadAsync(ms);

                var fileAccessControl = await fileClient.GetAccessControlAsync();

                var accessControlList = PathAccessControlExtensions.ParseAccessControlList(
                    "user::rwx,group::rwx,other::rw-");
                await fileClient.SetAccessControlListAsync((accessControlList));
                
                var uploadedVer = file.ToJson() != null;

                return uploadedVer;
            }
            catch (Exception e)
            {
                // log exception
                this.logger.LogError("Change Stream watcher. Exception:" + e.Message);
                throw;
            }
        }

        /// <summary>
        /// Upload data to Blob storage 
        /// </summary>
        /// <param name="updatedDocument"></param>
        /// <returns></returns>
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
                this.logger.LogError("UpdateBlobStorage. Exception:" + e.Message);
                throw;
            }
        }

        #endregion
    }
}
