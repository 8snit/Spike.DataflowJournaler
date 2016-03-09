using System;
using System.Threading.Tasks.Dataflow;

namespace Spike.DataflowJournaler
{
    public interface IJournalable
    {
        object Target { get; }

        DateTimeOffset Timestamp { set; }

        int Index { set; }
    }

    public class Journalable : IJournalable
    {
        public Journalable(object target, WriteOnceBlock<DateTimeOffset> timestampBlock)
        {
            Target = target;

            TimestampBlock = timestampBlock;
        }

        public WriteOnceBlock<DateTimeOffset> TimestampBlock { get; }

        public object Target { get; }

        public DateTimeOffset Timestamp
        {
            set { TimestampBlock.Post(value); }
        }

        public int Index
        {
            set
            {
                /* not used currently */
            }
        }
    }
}