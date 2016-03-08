namespace Spike.DataflowJournaler
{
    public class JournalConfig
    {
        public long CommandBatchTimeoutInMilliseconds { get; set; } = 100;

        public int CommandBatchSize { get; set; } = 1;

        public string Directory { get; set; }

        public long MaxSizeInBytes { get; set; } = 1024*1024*10;
    }
}