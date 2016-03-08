using System;
using System.Collections.Generic;
using System.Linq;

namespace Spike.DataflowJournaler
{
    public struct JournalStatisticEntry
    {
        public int BatchSize { get; set; }

        public TimeSpan OverallElapsed { get; set; }

        public TimeSpan WritingOnlyElapsed { get; set; }
    }

    public class JournalStatistic
    {
        public List<JournalStatisticEntry> Entries { get; } = new List<JournalStatisticEntry>();

        public void Dump(Action<string> writeLine)
        {
            var groups = Entries.GroupBy(
                entry => entry.BatchSize,
                entry => entry).OrderBy(e => e.Key);
            writeLine(string.Format("histogram ({0})", groups.Count()));
            foreach (var group in groups)
            {
                writeLine(string.Format("{0,5}: elapsedMS={1}({2}/{3}) maxDelayMs={4}",
                    group.Key,
                    group.Average(e => e.OverallElapsed.TotalMilliseconds),
                    group.Min(e => e.OverallElapsed.TotalMilliseconds),
                    group.Max(e => e.OverallElapsed.TotalMilliseconds),
                    group.Max(e => e.OverallElapsed - e.WritingOnlyElapsed).TotalMilliseconds));
            }
        }
    }
}