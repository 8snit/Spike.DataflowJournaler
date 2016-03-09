using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Spike.DataflowJournaler
{
    public interface IJournalPersistor : IDisposable
    {
        Task WriteAsync(DateTimeOffset timestamp, params IJournalable[] journalables);

        Task<DateTimeOffset> ReadLatestTimestampAsync(CancellationToken cancellationToken);

        Task<TTarget[]> ReadAsync<TTarget>(DateTimeOffset firstTimestamp, DateTimeOffset lastTimestamp,
            CancellationToken cancellationToken);
    }

    public class JournalPersistor : IJournalPersistor
    {
        private static readonly JsonSerializer JsonSerializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            Binder = new CustomSerializationBinder(),
            PreserveReferencesHandling = PreserveReferencesHandling.None,
            TypeNameHandling = TypeNameHandling.All,
            TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple
        });

        private readonly string _directory;

        private readonly Lazy<List<JournalFile>> _journalFiles;

        private readonly JournalReader _journalReader;

        private readonly JournalWriter _journalWriter;

        private readonly long _maxSizeInBytes;

        private readonly Func<DateTimeOffset?, Stream> _streamProvisioning;

        private Stream _stream;

        public JournalPersistor(string directory, long maxSizeInBytes)
        {
            _directory = directory;
            _maxSizeInBytes = maxSizeInBytes;
            _journalReader = new JournalReader(JsonSerializer);
            _journalWriter = new JournalWriter(JsonSerializer);
            _streamProvisioning = cutoffTimestamp =>
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
                var path = Path.Combine(_directory, currentJournalFile.Filename);
                return new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
            };
            _journalFiles = new Lazy<List<JournalFile>>(() =>
            {
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                var journalFiles = new List<JournalFile>();
                foreach (var file in Directory.GetFiles(directory, "*.journal"))
                {
                    var fileName = new FileInfo(file).Name;
                    journalFiles.Add(JournalFile.Parse(fileName));
                }

                journalFiles.Sort((a, b) => a.FileSequenceNumber.CompareTo(b.FileSequenceNumber));
                return journalFiles;
            });
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

        public Task WriteAsync(DateTimeOffset timestamp, params IJournalable[] journalables)
        {
            if (_stream == null)
            {
                _stream = _streamProvisioning.Invoke(null);
            }

            if (_stream.Position >= _maxSizeInBytes)
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
                _journalWriter.Write(_stream, timestamp, journalables);
            }

            return Task.FromResult(0);
        }

        public Task<DateTimeOffset> ReadLatestTimestampAsync(CancellationToken cancellationToken)
        {
            var journalFile = _journalFiles.Value.LastOrDefault();
            if (journalFile == null)
            {
                return Task.FromResult(DateTimeOffset.MinValue);
            }

            var path = Path.Combine(_directory, journalFile.Filename);
            using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new TaskCanceledException();
                }

                return Task.FromResult(_journalReader.ReadLatestTimestamp(stream));
            }
        }

        public Task<TTarget[]> ReadAsync<TTarget>(DateTimeOffset firstTimestamp, DateTimeOffset lastTimestamp,
            CancellationToken cancellationToken)
        {
            var targets = new List<TTarget>();
            for (var index = 0; index < _journalFiles.Value.Count; index++)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new TaskCanceledException();
                }

                if (_journalFiles.Value[index].FirstTimestamp > lastTimestamp)
                {
                    break;
                }

                if (index + 1 < _journalFiles.Value.Count
                    && _journalFiles.Value[index + 1].FirstTimestamp > firstTimestamp)
                {
                    continue;
                }

                var path = Path.Combine(_directory, _journalFiles.Value[index].Filename);
                using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new TaskCanceledException();
                    }

                    targets.AddRange(_journalReader.Read<TTarget>(stream, firstTimestamp, lastTimestamp));
                }
            }
            return Task.FromResult(targets.ToArray());
        }

        private class JournalWriter
        {
            private readonly JsonSerializer _jsonSerializer;

            public JournalWriter(JsonSerializer jsonSerializer)
            {
                _jsonSerializer = jsonSerializer;
            }

            public void Write(Stream stream, DateTimeOffset timestamp, params IJournalable[] journalables)
            {
                var streamWriter = new StreamWriter(stream);
                var jsonTextWriter = new JsonTextWriter(streamWriter);
                jsonTextWriter.WriteStartObject();
                jsonTextWriter.WritePropertyName("t");
                jsonTextWriter.WriteValue(timestamp.UtcTicks);
                jsonTextWriter.WritePropertyName("c");
                jsonTextWriter.WriteStartArray();
                foreach (var journalable in journalables)
                {
                    _jsonSerializer.Serialize(jsonTextWriter, journalable.Target);
                }
                jsonTextWriter.WriteEndArray();
                jsonTextWriter.WriteEndObject();
                jsonTextWriter.Flush();
                streamWriter.WriteLine();
                streamWriter.Flush();
                var index = 0;
                foreach (var journalable in journalables)
                {
                    journalable.Timestamp = timestamp;
                    journalable.Index = index++;
                }
            }
        }

        private class JournalReader
        {
            private readonly JsonSerializer _jsonSerializer;

            public JournalReader(JsonSerializer jsonSerializer)
            {
                _jsonSerializer = jsonSerializer;
            }

            public DateTimeOffset ReadLatestTimestamp(Stream stream)
            {
                var streamReader = new StreamReader(stream);
                var encoding = streamReader.CurrentEncoding;
                var charSize = encoding.GetByteCount(Environment.NewLine);
                var newLineBuffer = encoding.GetBytes(Environment.NewLine);

                var offset = charSize;
                while (true)
                {
                    offset += 1;
                    stream.Seek(-offset, SeekOrigin.End);

                    if (stream.Position == 0)
                    {
                        break;
                    }

                    stream.Read(newLineBuffer, 0, newLineBuffer.Length);
                    if (encoding.GetString(newLineBuffer) == Environment.NewLine)
                    {
                        break;
                    }
                }

                var lastLineBuffer = new byte[stream.Length - stream.Position];
                stream.Read(lastLineBuffer, 0, lastLineBuffer.Length);
                var lastLine = encoding.GetString(lastLineBuffer);

                if (string.IsNullOrEmpty(lastLine))
                {
                    return DateTimeOffset.MinValue;
                }

                var jsonTextReader = new JsonTextReader(new StringReader(lastLine));
                jsonTextReader.Read(); // start
                jsonTextReader.Read(); // 't'
                jsonTextReader.Read();
                var utcTicks = (long) jsonTextReader.Value;
                return new DateTimeOffset(utcTicks, TimeSpan.Zero);
            }

            public IEnumerable<TTarget> Read<TTarget>(Stream stream, DateTimeOffset firstTimestamp,
                DateTimeOffset lastTimestamp)
            {
                var streamReader = new StreamReader(stream);
                while (!streamReader.EndOfStream)
                {
                    var line = streamReader.ReadLine();
                    if (string.IsNullOrEmpty(line))
                    {
                        continue;
                    }

                    var jsonTextReader = new JsonTextReader(new StringReader(line));
                    jsonTextReader.Read(); // start
                    jsonTextReader.Read(); // 't'
                    jsonTextReader.Read();
                    var utcTicks = (long) jsonTextReader.Value;
                    if (utcTicks < firstTimestamp.UtcTicks)
                    {
                        continue;
                    }
                    if (utcTicks > lastTimestamp.UtcTicks)
                    {
                        break;
                    }

                    jsonTextReader.Read(); // 'c'
                    jsonTextReader.Read();
                    var objects = _jsonSerializer.Deserialize<object[]>(jsonTextReader);
                    foreach (var target in objects.OfType<TTarget>())
                    {
                        yield return target;
                    }
                }
            }
        }
    }
}