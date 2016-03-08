using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;

namespace Spike.DataflowJournaler
{
    public interface IJournalPersistor
    {
        void Write(Stream stream, DateTimeOffset timestamp, IJournalable[] journalables);

        IEnumerable<TTarget> Read<TTarget>(Stream stream, DateTimeOffset firstTimestamp, DateTimeOffset lastTimestamp);

        DateTimeOffset ReadLatestTimestamp(Stream stream);
    }

    public class JournalPersistor : IJournalPersistor
    {
        private readonly JsonSerializer _jsonSerializer;

        public JournalPersistor(JsonSerializer jsonSerializer)
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
                journalable.TimestampBlock.Post(timestamp);
                _jsonSerializer.Serialize(jsonTextWriter, journalable.Target);
            }
            jsonTextWriter.WriteEndArray();
            jsonTextWriter.WriteEndObject();
            jsonTextWriter.Flush();
            streamWriter.WriteLine();
            streamWriter.Flush();
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