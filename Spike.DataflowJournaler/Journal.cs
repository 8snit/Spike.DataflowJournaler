using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;
using Timer = System.Timers.Timer;

namespace Spike.DataflowJournaler
{
    public class Journal : IDisposable
    {
        private readonly JournalConfig _journalConfig;

        private readonly Lazy<List<JournalFile>> _journalFiles;

        private readonly ActionBlock<IJournalable[]> _journalingBlock;

        private readonly IJournalPersistor _journalPersistor;

        private readonly BatchBlock<IJournalable> _requestBlock;

        private readonly Timer _timer;

        public Journal(JournalConfig journalConfig)
        {
            _journalConfig = journalConfig;
            _journalPersistor = new JournalPersistor(JsonSerializer.Create(new JsonSerializerSettings
            {
                Binder = new CustomSerializationBinder(),
                PreserveReferencesHandling = PreserveReferencesHandling.None,
                TypeNameHandling = TypeNameHandling.All,
                TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple
            }));
            _journalFiles = new Lazy<List<JournalFile>>(() =>
            {
                if (!Directory.Exists(_journalConfig.Directory))
                {
                    Directory.CreateDirectory(_journalConfig.Directory);
                }

                var journalFiles = new List<JournalFile>();
                foreach (var file in Directory.GetFiles(_journalConfig.Directory, "*.journal"))
                {
                    var fileName = new FileInfo(file).Name;
                    journalFiles.Add(JournalFile.Parse(fileName));
                }

                journalFiles.Sort((a, b) => a.FileSequenceNumber.CompareTo(b.FileSequenceNumber));
                return journalFiles;
            });

            _journalingBlock = new ActionBlock<IJournalable[]>(journalables =>
            {
                var journalStatisticEntry = new JournalStatisticEntry {BatchSize = journalables.Length};
                journalStatisticEntry.OverallElapsed = StopWatchUtil.Measure(() =>
                {
                    using (var journalWriter = new JournalWriter(_journalPersistor, cutoffTimestamp =>
                    {
                        var currentJournalFile = _journalFiles.Value.LastOrDefault();
                        if (currentJournalFile == null)
                        {
                            currentJournalFile = new JournalFile(1, cutoffTimestamp ?? DateTimeOffset.MinValue);
                            _journalFiles.Value.Add(currentJournalFile);
                        }
                        else if (cutoffTimestamp.HasValue)
                        {
                            currentJournalFile = new JournalFile(currentJournalFile.FileSequenceNumber + 1,
                                cutoffTimestamp.Value);
                            _journalFiles.Value.Add(currentJournalFile);
                        }
                        var path = Path.Combine(_journalConfig.Directory, currentJournalFile.Filename);
                        return new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
                    }, _journalConfig.MaxSizeInBytes))
                    {
                        journalStatisticEntry.WritingOnlyElapsed =
                            StopWatchUtil.Measure(() => { journalWriter.Write(DateTimeOffset.Now, journalables); });
                    }
                });
                Statistic.Entries.Add(journalStatisticEntry);
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1
            });
            _requestBlock = new BatchBlock<IJournalable>(journalConfig.CommandBatchSize);
            _requestBlock.LinkTo(_journalingBlock, new DataflowLinkOptions
            {
                PropagateCompletion = true
            });
            _timer = new Timer();
            _timer.Elapsed += (sender, args) => { _requestBlock.TriggerBatch(); };
            _timer.Interval = _journalConfig.CommandBatchTimeoutInMilliseconds;
            _timer.Enabled = true;
        }

        public DateTimeOffset ReadLatestTimestamp
        {
            get
            {
                var journalFile = _journalFiles.Value.LastOrDefault();
                if (journalFile == null)
                {
                    return DateTimeOffset.MinValue;
                }

                var path = Path.Combine(_journalConfig.Directory, journalFile.Filename);
                using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    return _journalPersistor.ReadLatestTimestamp(stream);
                }
            }
        }

        public JournalStatistic Statistic { get; } = new JournalStatistic();

        public void Dispose()
        {
            _timer.Enabled = false;
            _timer.Dispose();
            _requestBlock.Complete();
            _journalingBlock.Completion.Wait();
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
                    for (var index = 0; index < _journalFiles.Value.Count; index++)
                    {
                        if (_journalFiles.Value[index].FirstTimestamp > lastTimestamp)
                        {
                            break;
                        }

                        if (index + 1 < _journalFiles.Value.Count
                            && _journalFiles.Value[index + 1].FirstTimestamp > firstTimestamp)
                        {
                            continue;
                        }

                        var path = Path.Combine(_journalConfig.Directory, _journalFiles.Value[index].Filename);
                        using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                        {
                            var targets = _journalPersistor.Read<TTarget>(stream,
                                firstTimestamp ?? DateTimeOffset.MinValue, lastTimestamp ?? DateTimeOffset.MaxValue);
                            foreach (var target in targets)
                            {
                                if (predicate != null
                                    && !predicate(target))
                                {
                                    continue;
                                }

                                await targetBlock.SendAsync(target, cancellationToken ?? CancellationToken.None);
                            }
                        }
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