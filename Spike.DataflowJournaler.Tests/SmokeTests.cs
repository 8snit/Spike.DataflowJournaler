using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Spike.DataflowJournaler.Tests
{
    [TestFixture]
    public class SmokeTests
    {
        [SetUp]
        public void Init()
        {
            if (Directory.Exists(TestDirectory))
            {
                Directory.Delete(TestDirectory, true);
            }
        }

        private string TestDirectory
            => Path.Combine(TestContext.CurrentContext.TestDirectory, TestContext.CurrentContext.Test.Name);

        [Test]
        public void TestBatch()
        {
            using (var journal = new Journal(TestDirectory, batchSize: 100))
            {
                var t1 = journal.AddAsync(new Transaction(Operation.Deposit, 1));
                var t2 = journal.AddAsync(new Transaction(Operation.Deposit, 2));
                Thread.Sleep(200);
                var t3 = journal.AddAsync(new Transaction(Operation.Deposit, 3));
                Task[] tasks = {t1, t2, t3};
                Task.WaitAll(tasks);

                Assert.IsTrue(t1.Result == t2.Result);
                Assert.IsTrue(t1.Result < t3.Result);
                Assert.AreEqual(6, TransactionsHelper.ComputeTotal(journal));
            }
        }

        [Test]
        public void TestFilter()
        {
            using (var journal = new Journal(TestDirectory, batchSize: 1))
            {
                var t1 = journal.AddAsync(new Transaction(Operation.Deposit, 1));
                var t2 = journal.AddAsync(new Transaction(Operation.Deposit, 2));
                var t3 = journal.AddAsync(new Transaction(Operation.Withdrawal, 1));
                var t4 = journal.AddAsync(new Transaction(Operation.Deposit, 3));
                Task[] tasks = {t1, t2, t3, t4};
                Task.WaitAll(tasks);

                Assert.AreEqual(5, TransactionsHelper.ComputeTotal(journal));
                Assert.AreEqual(6,
                    TransactionsHelper.ComputeTotal(journal,
                        predicate: transaction => transaction.Operation == Operation.Deposit));
                Assert.AreEqual(-1,
                    TransactionsHelper.ComputeTotal(journal,
                        predicate: transaction => transaction.Operation == Operation.Withdrawal));
            }
        }

        [Test]
        public async Task TestHelloWorld()
        {
            using (var journal = new Journal(TestDirectory))
            {
                await journal.AddAsync("Hello");
                await journal.AddAsync(" ");
                await journal.AddAsync("World");
                await journal.AddAsync("!");

                var message = string.Empty;
                journal.Replay<string>().Subscribe(item => message += item);
                Assert.AreEqual("Hello World!", message);
            }
        }

        [Test]
        public void TestNoBatch()
        {
            using (var journal = new Journal(TestDirectory))
            {
                var t1 = journal.AddAsync(new Transaction(Operation.Deposit, 1));
                Thread.Sleep(100);
                var t2 = journal.AddAsync(new Transaction(Operation.Deposit, 2));
                Thread.Sleep(100);
                var t3 = journal.AddAsync(new Transaction(Operation.Deposit, 3));
                Task[] tasks = {t1, t2, t3};
                Task.WaitAll(tasks);

                Assert.AreEqual(6, TransactionsHelper.ComputeTotal(journal));
                Assert.AreEqual(3, TransactionsHelper.ComputeTotal(journal, lastTimestamp: t2.Result));
                Assert.AreEqual(5, TransactionsHelper.ComputeTotal(journal, t2.Result));
                Assert.AreEqual(2, TransactionsHelper.ComputeTotal(journal, t2.Result, t2.Result));
                Assert.AreEqual(0, TransactionsHelper.ComputeTotal(journal, t3.Result.AddMilliseconds(1)));
                Assert.AreEqual(0,
                    TransactionsHelper.ComputeTotal(journal, lastTimestamp: t1.Result.AddMilliseconds(-1)));
            }
        }

        [Test]
        public void TestPersistence()
        {
            using (var journal = new Journal(TestDirectory))
            {
                var t1 = journal.AddAsync(new Transaction(Operation.Deposit, 1));
                var t2 = journal.AddAsync(new Transaction(Operation.Deposit, 2));
                var t3 = journal.AddAsync(new Transaction(Operation.Withdrawal, 1));
                var t4 = journal.AddAsync(new Transaction(Operation.Deposit, 3));
                Task[] tasks = {t1, t2, t3, t4};
                Task.WaitAll(tasks);

                Assert.AreEqual(5, TransactionsHelper.ComputeTotal(journal));
            }

            using (var journal = new Journal(TestDirectory))
            {
                Assert.AreEqual(5, TransactionsHelper.ComputeTotal(journal));
            }
        }
    }
}