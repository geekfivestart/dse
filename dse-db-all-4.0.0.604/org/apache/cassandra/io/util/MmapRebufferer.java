package org.apache.cassandra.io.util;

import com.google.common.primitives.Ints;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.concurrent.TPCUtils;

public class MmapRebufferer extends AbstractReaderFileProxy implements Rebufferer, RebuffererFactory {
   protected final MmappedRegions regions;

   MmapRebufferer(AsynchronousChannelProxy channel, long fileLength, MmappedRegions regions) {
      super(channel, fileLength);
      this.regions = regions;
   }

   public Rebufferer.BufferHolder rebuffer(long position) {
      return this.regions.floor(position);
   }

   public CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long position) {
      return TPCUtils.completedFuture(this.rebuffer(position));
   }

   public int rebufferSize() {
      return Ints.checkedCast(this.fileLength);
   }

   public Rebufferer.BufferHolder rebuffer(long position, Rebufferer.ReaderConstraint constraint) {
      return this.rebuffer(position);
   }

   public Rebufferer instantiateRebufferer(FileAccessType accessType) {
      return this;
   }

   public void close() {
      this.regions.closeQuietly();
   }

   public void closeReader() {
   }

   public String toString() {
      return String.format("%s(%s - data length %d)", new Object[]{this.getClass().getSimpleName(), this.channel.filePath(), Long.valueOf(this.fileLength())});
   }
}
