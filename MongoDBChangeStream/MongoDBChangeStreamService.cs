using System;
using System.Threading;
using System.Threading.Tasks;
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

        #region Public Methods
        public MongoDBChangeStreamService(IMongoClient client, IAppLogger<MongoDBChangeStreamService> logger, EventGridPublisherService eventGridPublisherService, IConfiguration configuration)
        {
            this.database = client.GetDatabase(configuration["mongodb-database"]);
            this.collection = this.database.GetCollection<BsonDocument>(configuration["mongodb-collection"]);
            this.eventGridPublisherService = eventGridPublisherService;
            this.client = client;
            this.logger = logger;
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
                    var updatedDocument = BsonSerializer.Deserialize<object>(change.FullDocument);

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
                    var Issucess = await eventGridPublisherService.EventGridPublisher(eventDetails);                    

                    // log information
                    this.logger.LogInformation($"Changes tracked successfully by change stream : {eventDetails.Data}");                    

                }
                catch (MongoException exception)
                {
                    // log mongo exception - helpful for developers
                    this.logger.LogError("Change Stream watcher. Exception:" + exception.Message);
                }
            });
        }
        #endregion
    }
}
