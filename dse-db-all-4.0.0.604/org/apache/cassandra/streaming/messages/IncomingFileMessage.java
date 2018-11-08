package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.compress.CompressedStreamReader;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class IncomingFileMessage extends StreamMessage {
   public static StreamMessage.Serializer<IncomingFileMessage> serializer = new StreamMessage.Serializer<IncomingFileMessage>() {
      public IncomingFileMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         DataInputPlus input = new DataInputPlus.DataInputStreamPlus(Channels.newInputStream(in));
         FileMessageHeader header = ((FileMessageHeader.FileMessageHeaderSerializer)FileMessageHeader.serializers.get(version)).deserialize(input);
         Object reader = !header.isCompressed()?new StreamReader(header, session):new CompressedStreamReader(header, session);

         try {
            return new IncomingFileMessage(((StreamReader)reader).read(in), header);
         } catch (Throwable var8) {
            JVMStabilityInspector.inspectThrowable(var8);
            throw var8;
         }
      }

      public void serialize(IncomingFileMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) {
         throw new UnsupportedOperationException("Not allowed to call serialize on an incoming file");
      }
   };
   public FileMessageHeader header;
   public SSTableMultiWriter sstable;

   public IncomingFileMessage(SSTableMultiWriter sstable, FileMessageHeader header) {
      super(StreamMessage.Type.FILE);
      this.header = header;
      this.sstable = sstable;
   }

   public String toString() {
      return "File (" + this.header + ", file: " + this.sstable.getFilename() + ")";
   }
}
