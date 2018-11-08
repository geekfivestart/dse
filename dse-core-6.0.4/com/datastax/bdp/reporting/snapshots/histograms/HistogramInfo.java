package com.datastax.bdp.reporting.snapshots.histograms;

import com.datastax.bdp.reporting.CqlWritable;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;

public class HistogramInfo implements CqlWritable {
   public final String keyspace;
   public final String table;
   public final long droppedMutations;
   private final long[] offsets;
   private final long[] histogramData;
   private final EstimatedHistogram estimatedHistogram;

   public HistogramInfo(String keyspace, String table, long[] histogramData, long droppedMutations) {
      this.keyspace = keyspace;
      this.table = table;
      this.histogramData = histogramData;
      if(histogramData.length == 0) {
         this.estimatedHistogram = null;
         this.offsets = null;
      } else {
         this.offsets = getBucketOffsets(histogramData);
         this.estimatedHistogram = new EstimatedHistogram(histogramData);
      }

      this.droppedMutations = droppedMutations;
   }

   public HistogramInfo(String keyspace, String table, long[] histogramData) {
      this(keyspace, table, histogramData, 0L);
   }

   public static long[] getBucketOffsets(long[] histogramData) {
      return histogramData.length == 0?null:(new EstimatedHistogram(histogramData.length, false)).getBucketOffsets();
   }

   public EstimatedHistogram getHistogram() {
      return this.estimatedHistogram;
   }

   public List<List<ByteBuffer>> toNestedByteBufferList() {
      int lowestIndex = -1;
      int highestIndex = -1;

      for(int i = 0; i < this.histogramData.length; ++i) {
         if(this.histogramData[i] > 0L) {
            highestIndex = i;
            if(lowestIndex == -1) {
               lowestIndex = i;
            }
         }
      }

      if(lowestIndex == -1) {
         return Collections.emptyList();
      } else {
         List<List<ByteBuffer>> offsetValues = new LinkedList();

         for(int i = lowestIndex; i <= highestIndex; ++i) {
            List<ByteBuffer> list = Lists.newLinkedList();
            list.add(ByteBufferUtil.bytes(this.offsets[i]));
            list.add(ByteBufferUtil.bytes(this.histogramData[i]));
            offsetValues.add(list);
         }

         return offsetValues;
      }
   }
}
