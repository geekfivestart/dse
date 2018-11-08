package org.apache.cassandra.utils.streamhist;

import java.io.IOException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class TombstoneHistogram {
   public static final TombstoneHistogram.HistogramSerializer serializer = new TombstoneHistogram.HistogramSerializer();
   private final StreamingTombstoneHistogramBuilder.DataHolder bin;

   TombstoneHistogram(StreamingTombstoneHistogramBuilder.DataHolder holder) {
      this.bin = new StreamingTombstoneHistogramBuilder.DataHolder(holder);
   }

   public static TombstoneHistogram createDefault() {
      return new TombstoneHistogram(new StreamingTombstoneHistogramBuilder.DataHolder(0, 1));
   }

   public double sum(double b) {
      return this.bin.sum((int)b);
   }

   public int size() {
      return this.bin.size();
   }

   public <E extends Exception> void forEach(HistogramDataConsumer<E> histogramDataConsumer) throws E {
      this.bin.forEach(histogramDataConsumer);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof TombstoneHistogram)) {
         return false;
      } else {
         TombstoneHistogram that = (TombstoneHistogram)o;
         return this.bin.equals(that.bin);
      }
   }

   public int hashCode() {
      return this.bin.hashCode();
   }

   public static class HistogramSerializer implements ISerializer<TombstoneHistogram> {
      public HistogramSerializer() {
      }

      public void serialize(TombstoneHistogram histogram, DataOutputPlus out) throws IOException {
         int size = histogram.size();
         out.writeInt(size);
         out.writeInt(size);
         histogram.forEach((point, value) -> {
            out.writeDouble((double)point);
            out.writeLong((long)value);
         });
      }

      public TombstoneHistogram deserialize(DataInputPlus in) throws IOException {
         in.readInt();
         int size = in.readInt();
         StreamingTombstoneHistogramBuilder.DataHolder dataHolder = new StreamingTombstoneHistogramBuilder.DataHolder(size, 1);

         for(int i = 0; i < size; ++i) {
            dataHolder.addValue((int)in.readDouble(), (int)in.readLong());
         }

         return new TombstoneHistogram(dataHolder);
      }

      public long serializedSize(TombstoneHistogram histogram) {
         int maxBinSize = 0;
         long size = (long)TypeSizes.sizeof((int)maxBinSize);
         int histSize = histogram.size();
         size += (long)TypeSizes.sizeof(histSize);
         size += (long)histSize * 16L;
         return size;
      }
   }
}
