using System;
using System.Threading.Tasks.Dataflow;

namespace Spike.DataflowJournaler
{
    public interface IJournalable
    {
        object Target { get; }

        WriteOnceBlock<DateTimeOffset> TimestampBlock { get; }
    }

    public class Journalable : IJournalable
    {
        public Journalable(object target, WriteOnceBlock<DateTimeOffset> timestampBlock)
        {
            Target = target;

            TimestampBlock = timestampBlock;
        }

        public object Target { get; }

        public WriteOnceBlock<DateTimeOffset> TimestampBlock { get; }
    }
}