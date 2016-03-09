using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Timer = System.Timers.Timer;

namespace Spike.DataflowJournaler
{
    public class Journal : IDisposable
    {
        private readonly ActionBlock<IJournalable[]> _journalingBlock;

        private readonly IJournalPersistor _journalPersistor;

        private readonly BatchBlock<IJournalable> _requestBlock;

        private readonly Timer _timer;

        public Journal(string directory, int maxSizeInByte = 10*1024*1024, long batchDelayMs = 10L, int batchSize = 100)
        {
            _journalPersistor = new JournalPersistor(directory, maxSizeInByte);
            _journalingBlock = new ActionBlock<IJournalable[]>(journalables =>
            {
                var journalStatisticEntry = new JournalStatisticEntry {BatchSize = journalables.Length};
                journalStatisticEntry.OverallElapsed = StopWatchUtil.Measure(() =>
                {
                    journalStatisticEntry.WritingOnlyElapsed =
                        StopWatchUtil.Measure(
                            () => { _journalPersistor.WriteAsync(DateTimeOffset.Now, journalables); });
                });
                Statistic.Entries.Add(journalStatisticEntry);
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

        public JournalStatistic Statistic { get; } = new JournalStatistic();

        public void Dispose()
        {
            _timer.Enabled = false;
            _timer.Dispose();
            _requestBlock.Complete();
            _journalingBlock.Completion.Wait();
            _journalPersistor.Dispose();
        }

        public Task<DateTimeOffset> AddAsync<TTarget>(TTarget target)
        {
            var timestampBlock = new WriteOnceBlock<DateTimeOffset>(r => r);
            _requestBlock.Post(new Journalable(target, timestampBlock));
            return timestampBlock.ReceiveAsync();
        }

        public Action<ITargetBlock<TTarget>> Replay<TTarget>(DateTimeOffset? firstTimestamp = null,
            DateTimeOffset? lastTimestamp = null, Func<TTarget, bool> predicate = null,
            CancellationToken? cancellationToken = null)
        {
            return async targetBlock =>
            {
                try
                {
                    var targets =
                        await
                            _journalPersistor.ReadAsync<TTarget>(firstTimestamp ?? DateTimeOffset.MinValue,
                                lastTimestamp ?? DateTimeOffset.MaxValue,
                                cancellationToken ?? CancellationToken.None);
                    foreach (var target in targets)
                    {
                        if (predicate != null
                            && !predicate(target))
                        {
                            continue;
                        }

                        await targetBlock.SendAsync(target, cancellationToken ?? CancellationToken.None);
                    }

                    targetBlock.Complete();
                }
                catch (Exception ex)
                {
                    targetBlock.Fault(ex);
                }
            };
        }
    }
}