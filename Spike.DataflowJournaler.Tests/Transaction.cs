using System;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Spike.DataflowJournaler.Tests
{
    [JsonConverter(typeof (StringEnumConverter))]
    public enum Operation
    {
        Deposit,

        Withdrawal
    }

    [JsonObject(Id = "tx")]
    public class Transaction
    {
        public Transaction(Operation operation, int value)
        {
            Operation = operation;
            Value = value;
        }

        [JsonProperty(PropertyName = "op")]
        public Operation Operation { get; }

        [JsonProperty(PropertyName = "val")]
        public int Value { get; }

        public long Execute(long current)
        {
            switch (Operation)
            {
                case Operation.Deposit:
                    current += Value;
                    break;
                case Operation.Withdrawal:
                    current -= Value;
                    break;
            }

            return current;
        }
    }

    public static class TransactionsHelper
    {
        public static long ComputeTotal(Journal journal, DateTimeOffset? firstTimestamp = null,
            DateTimeOffset? lastTimestamp = null, Func<Transaction, bool> predicate = null)
        {
            var total = 0L;
            var actionBlock = new ActionBlock<Transaction>(transaction => { total = transaction.Execute(total); });
            journal.Replay(firstTimestamp, lastTimestamp, predicate)(actionBlock);
            actionBlock.Completion.Wait();
            return total;
        }
    }
}