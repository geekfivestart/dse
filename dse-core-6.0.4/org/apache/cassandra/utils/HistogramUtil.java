package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;

public class HistogramUtil {
   public static final byte[] EMPTY_HDR_HISTOGRAM_BYTES;
   public static final AbstractHistogram EMPTY_HDR_HISTOGRAM;

   public HistogramUtil() {
   }

   public static byte[] hdrHistogramBytes(AbstractHistogram histogram) {
      ByteBuffer outputBuffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
      int length = histogram.copy().encodeIntoCompressedByteBuffer(outputBuffer, 1);
      outputBuffer.position(0);
      outputBuffer.limit(length);
      byte[] compressedBytes = new byte[outputBuffer.remaining()];
      outputBuffer.get(compressedBytes);
      return compressedBytes;
   }

   public static EstimatedHistogram estimatedHistogramFromHdrHistogram(AbstractHistogram h) {
      EstimatedHistogram resultHistogram = new EstimatedHistogram(bucketsRequired(h), true);
      long[] offsets = resultHistogram.getBucketOffsets();

      HistogramIterationValue value;
      int index;
      for(Iterator iterator = h.allValues().iterator(); iterator.hasNext(); resultHistogram.buckets.addAndGet(index, value.getCountAddedInThisIterationStep())) {
         value = (HistogramIterationValue)iterator.next();
         long iteratedTo = value.getValueIteratedTo();
         index = Arrays.binarySearch(offsets, iteratedTo);
         if(index < 0) {
            index = -index - 1;
         }
      }

      return resultHistogram;
   }

   private static int bucketsRequired(AbstractHistogram h) {
      long largestValue = h.getMaxValue();
      int bucketsRequired = 1;
      long last = 1L;

      long next;
      do {
         next = Math.round((double)last * 1.2D);
         if(next == last) {
            ++next;
         }

         last = next;
         ++bucketsRequired;
      } while(next <= largestValue);

      return bucketsRequired;
   }

   static {
      EMPTY_HDR_HISTOGRAM_BYTES = hdrHistogramBytes(new Histogram(TimeUnit.SECONDS.toMicros(10L), 3));
      EMPTY_HDR_HISTOGRAM = new Histogram(TimeUnit.SECONDS.toMicros(10L), 3);
   }
}
