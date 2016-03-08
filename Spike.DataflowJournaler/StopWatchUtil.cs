using System;
using System.Diagnostics;

namespace Spike.DataflowJournaler
{
    public static class StopWatchUtil
    {
        public static TimeSpan Measure(Action action)
        {
            var stopwatch = Stopwatch.StartNew();
            action();
            stopwatch.Stop();
            return stopwatch.Elapsed;
        }
    }
}