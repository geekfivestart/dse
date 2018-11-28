package org.apache.cassandra.io.util;

import io.netty.channel.epoll.AIOEpollFileChannel;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.AIOContext.Batch;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsynchronousChannelProxy extends AbstractChannelProxy<AsynchronousFileChannel> {
   private static final Logger logger = LoggerFactory.getLogger(AsynchronousChannelProxy.class);
   private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];
   private static final Set<OpenOption> READ_ONLY;
   private static final ExecutorService javaAioGroup;
   public final AIOEpollFileChannel epollChannel;
   private static final int MAX_RETRIES = 10;

   private static AsynchronousFileChannel openFileChannel(File file, boolean javaAIO) {
      try {
         if(TPC.USE_AIO && !javaAIO) {
            try {
               int flags = 16384;
               return new AIOEpollFileChannel(file, (EpollEventLoop)TPC.bestIOEventLoop(), flags);
            } catch (IOException var3) {
               if(var3.getMessage().contains("Invalid argument")) {
                  return AsynchronousFileChannel.open(file.toPath(), READ_ONLY, javaAioGroup, NO_ATTRIBUTES);
               } else {
                  throw var3;
               }
            }
         } else {
            return AsynchronousFileChannel.open(file.toPath(), READ_ONLY, javaAioGroup, NO_ATTRIBUTES);
         }
      } catch (IOException var4) {
         throw new RuntimeException(var4);
      }
   }

   public AsynchronousChannelProxy(String path) {
      this(new File(path), false);
   }

   public AsynchronousChannelProxy(String path, boolean javaAIO) {
      this(new File(path), javaAIO);
   }

   public AsynchronousChannelProxy(File file, boolean javaAIO) {
      this(file.getPath(), openFileChannel(file, javaAIO));
   }

   public AsynchronousChannelProxy(String filePath, AsynchronousFileChannel channel) {
      super(filePath, channel);
      this.epollChannel = channel instanceof AIOEpollFileChannel?(AIOEpollFileChannel)channel:null;
   }

   public AsynchronousChannelProxy(AsynchronousChannelProxy copy) {
      super(copy);
      this.epollChannel = this.channel instanceof AIOEpollFileChannel?(AIOEpollFileChannel)this.channel:null;
   }

   boolean requiresAlignment() {
      return this.channel instanceof AIOEpollFileChannel;
   }

   CompletionHandler<Integer, ByteBuffer> makeRetryingHandler(final ByteBuffer dest, final long offset, final CompletionHandler<Integer, ByteBuffer> onComplete) {
      return new CompletionHandler<Integer, ByteBuffer>() {
         int retries = 0;

         public void completed(Integer result, ByteBuffer attachment) {
            onComplete.completed(result, attachment);
         }

         public void failed(Throwable exc, ByteBuffer attachment) {
            if(exc instanceof RuntimeException && exc.getMessage().contains("Too many pending requests")) {
               if(++this.retries < 10) {
                  AsynchronousChannelProxy.logger.warn("Got {}. Retrying {} more times.", exc.getMessage(), Integer.valueOf(10 - this.retries));
                  TPC.bestTPCScheduler().scheduleDirect(() -> {
                     ((AsynchronousFileChannel)AsynchronousChannelProxy.this.channel).read(dest, offset, dest, this);
                  }, (long)(1 << this.retries - 1), TimeUnit.MILLISECONDS);
                  return;
               }

               AsynchronousChannelProxy.logger.error("Got {} and exhausted all retries.", exc.getMessage());
            }

            AsynchronousChannelProxy.logger.debug("Failed to read {} with exception {}", AsynchronousChannelProxy.this.filePath, exc);
            onComplete.failed(exc, attachment);
         }
      };
   }

   public void read(ByteBuffer dest, long offset, CompletionHandler<Integer, ByteBuffer> onComplete) {
      CompletionHandler retryingHandler = this.makeRetryingHandler(dest, offset, onComplete);

      try {
         if(this.epollChannel != null) {
            this.epollChannel.read(dest, offset, dest, retryingHandler, (EpollEventLoop)TPC.bestIOEventLoop());
         } else {
            ((AsynchronousFileChannel)this.channel).read(dest, offset, dest, retryingHandler);
         }
      } catch (Throwable var7) {
         retryingHandler.failed(var7, dest);
      }

   }

   public long size() throws FSReadError {
      try {
         return ((AsynchronousFileChannel)this.channel).size();
      } catch (IOException var2) {
         throw new FSReadError(var2, this.filePath);
      }
   }

   public ChannelProxy getBlockingChannel() {
      return new ChannelProxy(new File(this.filePath));
   }

   public long transferTo(long position, long count, WritableByteChannel target) {
      ChannelProxy cp = this.getBlockingChannel();
      Throwable var7 = null;

      long var8;
      try {
         var8 = cp.transferTo(position, count, target);
      } catch (Throwable var18) {
         var7 = var18;
         throw var18;
      } finally {
         if(cp != null) {
            if(var7 != null) {
               try {
                  cp.close();
               } catch (Throwable var17) {
                  var7.addSuppressed(var17);
               }
            } else {
               cp.close();
            }
         }

      }

      return var8;
   }

   public AsynchronousChannelProxy sharedCopy() {
      return new AsynchronousChannelProxy(this);
   }

   public void tryToSkipCache(long offset, long len) {
      int fd;
      if(this.epollChannel != null) {
         if(this.epollChannel.isDirect()) {
            return;
         }

         fd = this.epollChannel.getFd();
      } else {
         fd = NativeLibrary.getfd((AsynchronousFileChannel)this.channel);
      }

      NativeLibrary.trySkipCache(fd, offset, len, this.filePath);
   }

   public void startBatch() {
   }

   public void submitBatch() {
   }

   public AsynchronousChannelProxy maybeBatched(boolean vectored) {
      return (AsynchronousChannelProxy)(this.epollChannel == null?this.sharedCopy():new AsynchronousChannelProxy.AIOEpollBatchedChannelProxy(this, vectored));
   }

   static {
      READ_ONLY = Collections.singleton(StandardOpenOption.READ);
      if(!TPC.USE_AIO && !FBUtilities.isWindows) {
         javaAioGroup = Executors.newFixedThreadPool(32, new NamedThreadFactory("java-aio"));
      } else {
         javaAioGroup = Executors.newCachedThreadPool(new NamedThreadFactory("java-aio"));
      }

   }

   private static class AIOEpollBatchedChannelProxy extends AsynchronousChannelProxy {
      private static final FastThreadLocal<Batch<ByteBuffer>> batch = new FastThreadLocal();
      private final boolean vectored;
      private final AIOEpollFileChannel epollChannel;

      private AIOEpollBatchedChannelProxy(AsynchronousChannelProxy inner, boolean vectored) {
         super(inner);
         this.vectored = vectored;
         this.epollChannel = (AIOEpollFileChannel)inner.channel;
      }

      public void read(ByteBuffer dest, long offset, CompletionHandler<Integer, ByteBuffer> onComplete) {
         CompletionHandler handler = this.makeRetryingHandler(dest, offset, onComplete);

         try {
            if(!batch.isSet()) {
               this.epollChannel.read(dest, offset, dest, handler, (EpollEventLoop)TPC.bestIOEventLoop());
            } else {
               ((Batch)batch.get()).add(offset, dest, dest, handler);
            }
         } catch (Throwable var7) {
            handler.failed(var7, dest);
         }

      }

      public void startBatch() {
         assert !batch.isSet() : "Batch was already started";

         batch.set(this.epollChannel.newBatch(this.vectored));
      }

      public void submitBatch() {
         if (!AIOEpollBatchedChannelProxy.batch.isSet()) {
            return;
         }
         final Batch<ByteBuffer> batch = (Batch<ByteBuffer>)AIOEpollBatchedChannelProxy.batch.get();
         try {
            if (batch.numRequests() > 0) {
               this.epollChannel.read((Batch)batch, (EpollEventLoop)TPC.bestIOEventLoop());
            }
         }
         catch (Throwable err) {
            batch.failed(err);
         }
         finally {
            AIOEpollBatchedChannelProxy.batch.remove();
         }
      }
   }
}
