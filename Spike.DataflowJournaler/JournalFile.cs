using System;
using System.Text.RegularExpressions;

namespace Spike.DataflowJournaler
{
    public class JournalFile
    {
        private static readonly Regex JournalFilenameParser =
            new Regex(@"^(?<fileSequenceNumber>\d{8}).(?<firstTimestamp>\d{19}).journal$");

        public JournalFile(long fileSequenceNumber, DateTimeOffset firstTimestamp)
        {
            FileSequenceNumber = fileSequenceNumber;
            FirstTimestamp = firstTimestamp;
        }

        public long FileSequenceNumber { get; }

        public DateTimeOffset FirstTimestamp { get; }

        public string Filename
        {
            get
            {
                return string.Format("{0:00000000}.{1:0000000000000000000}.journal", FileSequenceNumber,
                    FirstTimestamp.UtcTicks);
            }
        }

        public static JournalFile Parse(string filename)
        {
            var match = JournalFilenameParser.Match(filename);
            if (!match.Success)
            {
                throw new ArgumentException("invalid journal filename");
            }

            var fileSequenceNumber = TrimNumber(match.Groups["fileSequenceNumber"].Value);
            var firstTimestamp = new DateTimeOffset(TrimNumber(match.Groups["firstTimestamp"].Value), TimeSpan.Zero);
            return new JournalFile(fileSequenceNumber, firstTimestamp);
        }

        public JournalFile Successor(DateTimeOffset firstTimestamp)
        {
            return new JournalFile(FileSequenceNumber + 1, firstTimestamp);
        }

        private static long TrimNumber(string number)
        {
            var trimmedNumber = number.TrimStart('0');
            return string.IsNullOrEmpty(trimmedNumber) ? 0 : long.Parse(trimmedNumber);
        }
    }
}