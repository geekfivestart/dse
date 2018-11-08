package org.apache.cassandra.streaming;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import java.util.UUID;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public enum PreviewKind {
   NONE(0, (Predicate)null),
   ALL(1, Predicates.alwaysTrue()),
   UNREPAIRED(2, Predicates.not(SSTableReader::isRepaired)),
   REPAIRED(3, SSTableReader::isRepaired);

   private final int serializationVal;
   private final Predicate<SSTableReader> streamingPredicate;

   private PreviewKind(int serializationVal, Predicate<SSTableReader> streamingPredicate) {
      assert this.ordinal() == serializationVal;

      this.serializationVal = serializationVal;
      this.streamingPredicate = streamingPredicate;
   }

   public int getSerializationVal() {
      return this.serializationVal;
   }

   public static PreviewKind deserialize(int serializationVal) {
      return values()[serializationVal];
   }

   public Predicate<SSTableReader> getStreamingPredicate() {
      return this.streamingPredicate;
   }

   public boolean isPreview() {
      return this != NONE;
   }

   public String logPrefix() {
      return this.isPreview()?"preview repair":"repair";
   }

   public String logPrefix(UUID sessionId) {
      return '[' + this.logPrefix() + " #" + sessionId.toString() + ']';
   }
}
