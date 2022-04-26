using System;
namespace MongoSourceConnectorToEventGrid.Models
{
    public class EventDetails
    {
        public string EventType { get; set; }
        public string Data { get; set; }
        public DateTime EventTime { get; set; }
        public string Subject { get; set; }
        public string Version { get; set; }
    }
}
