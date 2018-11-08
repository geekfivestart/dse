package org.apache.cassandra.streaming.messages;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamWriter;
import org.apache.cassandra.streaming.compress.CompressedStreamWriter;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

public class OutgoingFileMessage extends StreamMessage {
   public static StreamMessage.Serializer<OutgoingFileMessage> serializer = new StreamMessage.Serializer<OutgoingFileMessage>() {
      public OutgoingFileMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) {
         throw new UnsupportedOperationException("Not allowed to call deserialize on an outgoing file");
      }

      public void serialize(OutgoingFileMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         message.startTransfer();

         try {
            message.serialize(out, version, session);
            session.fileSent(message.header);
         } finally {
            message.finishTransfer();
         }

      }
   };
   public final FileMessageHeader header;
   private final Ref<SSTableReader> ref;
   private final String filename;
   private boolean completed = false;
   private boolean transferring = false;

   public OutgoingFileMessage(Ref<SSTableReader> ref, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections, boolean keepSSTableLevel) {
      super(StreamMessage.Type.FILE);
      this.ref = ref;
      SSTableReader sstable = (SSTableReader)ref.get();
      this.filename = sstable.getFilename();
      this.header = new FileMessageHeader(sstable.metadata().id, sequenceNumber, sstable.descriptor.version, sstable.descriptor.formatType, estimatedKeys, sections, sstable.compression?sstable.getCompressionMetadata():null, sstable.getRepairedAt(), sstable.getPendingRepair(), keepSSTableLevel?sstable.getSSTableLevel():0, sstable.header.toComponent());
   }

   public synchronized void serialize(DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
      if(!this.completed) {
         CompressionInfo compressionInfo = ((FileMessageHeader.FileMessageHeaderSerializer)FileMessageHeader.serializers.get(version)).serialize(this.header, out);
         SSTableReader reader = (SSTableReader)this.ref.get();
         StreamWriter writer = compressionInfo == null?new StreamWriter(reader, this.header.sections, session):new CompressedStreamWriter(reader, this.header.sections, compressionInfo, session);
         ((StreamWriter)writer).write(out);
      }
   }

   @VisibleForTesting
   public synchronized void finishTransfer() {
      this.transferring = false;
      if(this.completed) {
         this.ref.release();
      }

   }

   @VisibleForTesting
   public synchronized void startTransfer() {
      if(this.completed) {
         throw new RuntimeException(String.format("Transfer of file %s already completed or aborted (perhaps session failed?).", new Object[]{this.filename}));
      } else {
         this.transferring = true;
      }
   }

   public synchronized void complete() {
      if(!this.completed) {
         this.completed = true;
         if(!this.transferring) {
            this.ref.release();
         }
      }

   }

   public String toString() {
      return "File (" + this.header + ", file: " + this.filename + ")";
   }
}
