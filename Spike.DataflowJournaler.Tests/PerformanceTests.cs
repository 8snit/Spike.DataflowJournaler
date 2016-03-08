using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Spike.DataflowJournaler.Tests
{
    [TestFixture]
    public class PerformanceTests
    {
        [SetUp]
        public void Init()
        {
            if (Directory.Exists(TestDirectory))
            {
                Directory.Delete(TestDirectory, true);
            }
        }

        // number of buckets, size of bucket, degree of parallelism, size of batch
        public static IEnumerable<Tuple<int, int, int, int>> TestCases()
        {
            var batchSizes = new[] {1, 5, 50};
            var degreesOfParallelism = new[] {1, 2, 4};
            var bucketSizes = new[] {1, 10, 100, 1000};
            var totals = new[] {1000, 10000, 100000};

            foreach (var total in totals)
            {
                foreach (var bucketSize in bucketSizes)
                {
                    foreach (var degreeOfParallelism in degreesOfParallelism)
                    {
                        foreach (var batchSize in batchSizes)
                        {
                            yield return Tuple.Create(total/bucketSize, bucketSize, degreeOfParallelism, batchSize);
                        }
                    }
                }
            }
        }

        private string TestDirectory
            => Path.Combine(TestContext.CurrentContext.TestDirectory, TestContext.CurrentContext.Test.Name);

        [Test, TestCaseSource(nameof(TestCases))]
        public void TestPerformanceBatchSize(Tuple<int, int, int, int> testCase)
        {
            var total = (long) testCase.Item1*(testCase.Item1 + 1)/2*testCase.Item2;
            using (var journal = new Journal(new JournalConfig
            {
                Directory = TestDirectory,
                CommandBatchSize = testCase.Item4,
                CommandBatchTimeoutInMilliseconds = 10
            }))
            {
                var addingElapsedByThreadId = new ConcurrentDictionary<long, List<TimeSpan>>();
                Parallel.For(1, testCase.Item1 + 1, new ParallelOptions
                {
                    MaxDegreeOfParallelism = testCase.Item3
                }, value =>
                {
                    var addingElapsed = StopWatchUtil.Measure(() =>
                    {
                        var tasks =
                            Enumerable.Range(1, testCase.Item2)
                                .Select(_ => { return journal.AddAsync(new Transaction(Operation.Deposit, value)); })
                                .ToArray();
                        Task.WaitAll(tasks);
                    });
                    addingElapsedByThreadId.AddOrUpdate(
                        Thread.CurrentThread.ManagedThreadId,
                        key => { return new List<TimeSpan> {addingElapsed}; },
                        (key, list) =>
                        {
                            list.Add(addingElapsed);
                            return list;
                        });
                });
                var computingElapsed =
                    StopWatchUtil.Measure(() => { Assert.AreEqual(total, TransactionsHelper.ComputeTotal(journal)); });

                using (
                    var fileStream =
                        new FileStream(
                            Path.Combine(TestContext.CurrentContext.TestDirectory,
                                GetType().FullName + ".txt"), FileMode.Append))
                {
                    using (var streamWriter = new StreamWriter(fileStream))
                    {
                        streamWriter.WriteLine("***" + testCase + "***");
                        streamWriter.WriteLine("timestamp=" + DateTimeOffset.UtcNow);
                        streamWriter.WriteLine("threads=" + addingElapsedByThreadId.Keys.Count);
                        streamWriter.WriteLine("addingMS=" +
                                               addingElapsedByThreadId.Values.Sum(
                                                   v => v.Sum(timeSpan => timeSpan.TotalMilliseconds)));
                        streamWriter.WriteLine("computingMS=" + computingElapsed.TotalMilliseconds);
                        journal.Statistic.Dump(streamWriter.WriteLine);
                    }
                }
            }
        }
    }
}