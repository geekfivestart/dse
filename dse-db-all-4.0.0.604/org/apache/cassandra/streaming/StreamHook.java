package org.apache.cassandra.streaming;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.FBUtilities;

public interface StreamHook {
   StreamHook instance = createHook();

   OutgoingFileMessage reportOutgoingFile(StreamSession var1, SSTableReader var2, OutgoingFileMessage var3);

   void reportStreamFuture(StreamSession var1, StreamResultFuture var2);

   void reportIncomingFile(ColumnFamilyStore var1, SSTableMultiWriter var2, StreamSession var3, int var4);

   public static StreamHook createHook() {
      String className = System.getProperty("cassandra.stream_hook");
      return className != null?(StreamHook)FBUtilities.construct(className, StreamHook.class.getSimpleName()):new StreamHook() {
         public OutgoingFileMessage reportOutgoingFile(StreamSession session, SSTableReader sstable, OutgoingFileMessage message) {
            return message;
         }

         public void reportStreamFuture(StreamSession session, StreamResultFuture future) {
         }

         public void reportIncomingFile(ColumnFamilyStore cfs, SSTableMultiWriter writer, StreamSession session, int sequenceNumber) {
         }
      };
   }
}
