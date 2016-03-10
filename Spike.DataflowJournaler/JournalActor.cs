using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;
using System.Timers;

namespace Spike.DataflowJournaler
{
    public class JournalActor : IDisposable
    {
        private readonly ActionBlock<IJournalable[]> _journalingBlock;

        private readonly IJournalPersistor _journalPersistor;

        private readonly BatchBlock<IJournalable> _requestBlock;

        private readonly Timer _timer;

        public JournalActor(IJournalPersistor journalPersistor, long batchDelayMs = 10L, int batchSize = 100)
        {
            _journalPersistor = journalPersistor;
            _journalingBlock = new ActionBlock<IJournalable[]>(journalables =>
            {
                var journalStatisticEntry = new JournalStatisticEntry {BatchSize = journalables.Length};
                journalStatisticEntry.OverallElapsed = StopWatchUtil.Measure(() =>
                {
                    journalStatisticEntry.WritingOnlyElapsed =
                        StopWatchUtil.Measure(
                            () => { _journalPersistor.WriteAsync(DateTimeOffset.Now, journalables); });
                });
                Statistic.Add(journalStatisticEntry);
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1
            });
            _requestBlock = new BatchBlock<IJournalable>(batchSize);
            _requestBlock.LinkTo(_journalingBlock, new DataflowLinkOptions
            {
                PropagateCompletion = true
            });
            _timer = new Timer();
            _timer.Elapsed += (sender, args) => { _requestBlock.TriggerBatch(); };
            _timer.Interval = batchDelayMs;
            _timer.Enabled = true;
        }

        public List<JournalStatisticEntry> Statistic { get; } = new List<JournalStatisticEntry>();

        public ITargetBlock<IJournalable> RequestBlock => _requestBlock;

        public void Dispose()
        {
            _timer.Enabled = false;
            _timer.Dispose();
            _requestBlock.Complete();
            _journalingBlock.Completion.Wait();
            _journalPersistor.Dispose();
        }
    }
}