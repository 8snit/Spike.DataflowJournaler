using System;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Spike.DataflowJournaler
{
    public class Journal : IDisposable
    {
        private readonly JournalActor _journalActor;
        private readonly IJournalPersistor _journalPersistor;

        public Journal(string directory, int maxSizeInByte = 10*1024*1024, long batchDelayMs = 10L, int batchSize = 100)
        {
            _journalPersistor = new JournalPersistor(directory, maxSizeInByte);
            _journalActor = new JournalActor(_journalPersistor, batchDelayMs, batchSize);
        }

        public void Dispose()
        {
            _journalActor.Dispose();
            _journalPersistor.Dispose();
        }

        public Task<DateTimeOffset> AddAsync<TTarget>(TTarget target)
        {
            var timestampBlock = new WriteOnceBlock<DateTimeOffset>(r => r);
            _journalActor.RequestBlock.Post(new Journalable(target, timestampBlock));
            return timestampBlock.ReceiveAsync();
        }

        public IObservable<TTarget> Replay<TTarget>(DateTimeOffset? firstTimestamp = null,
            DateTimeOffset? lastTimestamp = null, Func<TTarget, bool> predicate = null,
            CancellationToken? cancellationToken = null)
        {
            return Observable.Create<TTarget>(
                async observer =>
                {
                    try
                    {
                        var targets = await _journalPersistor.ReadAsync<TTarget>(
                            firstTimestamp ?? DateTimeOffset.MinValue,
                            lastTimestamp ?? DateTimeOffset.MaxValue,
                            cancellationToken ?? CancellationToken.None);
                        foreach (var target in targets)
                        {
                            if (predicate != null
                                && !predicate(target))
                            {
                                continue;
                            }

                            if (cancellationToken.HasValue
                                && cancellationToken.Value.IsCancellationRequested)
                            {
                                observer.OnError(new TaskCanceledException());
                                return;
                            }
                            observer.OnNext(target);
                        }

                        observer.OnCompleted();
                    }
                    catch (Exception ex)
                    {
                        observer.OnError(ex);
                    }
                });
        }

        public void DumpStatistic(Action<string> writeLine)
        {
            var groups = _journalActor.Statistic.GroupBy(
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