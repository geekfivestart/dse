package org.apache.cassandra.io.util;

import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.concurrent.TPCUtils;

public class EmptyRebufferer implements Rebufferer, RebuffererFactory {
   AsynchronousChannelProxy channel;

   public EmptyRebufferer(AsynchronousChannelProxy channel) {
      this.channel = channel;
   }

   public AsynchronousChannelProxy channel() {
      return this.channel;
   }

   public long fileLength() {
      return 0L;
   }

   public double getCrcCheckChance() {
      return 0.0D;
   }

   public Rebufferer instantiateRebufferer(FileAccessType accessType) {
      return this;
   }

   public Rebufferer.BufferHolder rebuffer(long position) {
      return EMPTY;
   }

   public CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long position) {
      return TPCUtils.completedFuture(EMPTY);
   }

   public int rebufferSize() {
      return 0;
   }

   public Rebufferer.BufferHolder rebuffer(long position, Rebufferer.ReaderConstraint constraint) {
      return EMPTY;
   }

   public void close() {
   }

   public void closeReader() {
   }
}
