using System;
using System.IO;

namespace Spike.DataflowJournaler
{
    public class JournalWriter : IDisposable
    {
        private readonly IJournalPersistor _journalPersistor;
        private readonly long _maxJournalSizeInBytes;

        private readonly Func<DateTimeOffset?, Stream> _streamProvisioning;

        private Stream _stream;

        public JournalWriter(IJournalPersistor journalPersistor, Func<DateTimeOffset?, Stream> streamProvisioning,
            long maxJournalSizeInBytes)
        {
            _journalPersistor = journalPersistor;
            _streamProvisioning = streamProvisioning;
            _maxJournalSizeInBytes = maxJournalSizeInBytes;
        }

        public virtual void Dispose()
        {
            if (_stream != null)
            {
                if (_stream.CanWrite)
                {
                    _stream.Flush();
                }
                _stream.Dispose();
                _stream = null;
            }
        }

        public void Write(DateTimeOffset timestamp, params IJournalable[] journalables)
        {
            if (_stream == null)
            {
                _stream = _streamProvisioning.Invoke(null);
            }

            if (_stream.Position >= _maxJournalSizeInBytes)
            {
                if (_stream.CanWrite)
                {
                    _stream.Flush();
                }
                _stream.Dispose();
                _stream = _streamProvisioning.Invoke(timestamp);
            }

            if (_stream.CanWrite)
            {
                _journalPersistor.Write(_stream, timestamp, journalables);
            }
        }
    }
}