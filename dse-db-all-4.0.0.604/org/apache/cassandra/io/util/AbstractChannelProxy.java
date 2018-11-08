package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.WritableByteChannel;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseable;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

abstract class AbstractChannelProxy<T extends Channel> extends SharedCloseableImpl {
   protected final String filePath;
   protected final T channel;

   AbstractChannelProxy(String filePath, T channel) {
      super((RefCounted.Tidy)(new AbstractChannelProxy.Cleanup(filePath, channel)));
      this.filePath = filePath;
      this.channel = channel;
   }

   AbstractChannelProxy(AbstractChannelProxy<T> copy) {
      super((SharedCloseableImpl)copy);
      this.filePath = copy.filePath;
      this.channel = copy.channel;
   }

   public abstract SharedCloseable sharedCopy();

   public abstract long size() throws FSReadError;

   public abstract long transferTo(long var1, long var3, WritableByteChannel var5);

   public abstract void tryToSkipCache(long var1, long var3);

   public String filePath() {
      return this.filePath;
   }

   public String toString() {
      return this.filePath();
   }

   private static final class Cleanup implements RefCounted.Tidy {
      final String filePath;
      final Channel channel;

      Cleanup(String filePath, Channel channel) {
         this.filePath = filePath;
         this.channel = channel;
      }

      public String name() {
         return this.filePath;
      }

      public void tidy() {
         try {
            this.channel.close();
         } catch (IOException var2) {
            throw new FSReadError(var2, this.filePath);
         }
      }
   }
}
