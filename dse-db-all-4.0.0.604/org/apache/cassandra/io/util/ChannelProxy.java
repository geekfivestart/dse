package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.NativeLibrary;

public final class ChannelProxy extends AbstractChannelProxy<FileChannel> {
   public static FileChannel openChannel(File file) {
      try {
         return FileChannel.open(file.toPath(), new OpenOption[]{StandardOpenOption.READ});
      } catch (IOException var2) {
         throw new RuntimeException(var2);
      }
   }

   public ChannelProxy(String path) {
      this(new File(path));
   }

   public ChannelProxy(File file) {
      this(file.getPath(), openChannel(file));
   }

   public ChannelProxy(String filePath, FileChannel channel) {
      super(filePath, channel);
   }

   public ChannelProxy(ChannelProxy copy) {
      super(copy);
   }

   public ChannelProxy sharedCopy() {
      return new ChannelProxy(this);
   }

   public int read(ByteBuffer buffer, long position) {
      try {
         return ((FileChannel)this.channel).read(buffer, position);
      } catch (IOException var5) {
         throw new FSReadError(var5, this.filePath);
      }
   }

   public long transferTo(long position, long count, WritableByteChannel target) {
      try {
         return ((FileChannel)this.channel).transferTo(position, count, target);
      } catch (IOException var7) {
         throw new FSReadError(var7, this.filePath);
      }
   }

   public MappedByteBuffer map(MapMode mode, long position, long size) {
      try {
         return ((FileChannel)this.channel).map(mode, position, size);
      } catch (IOException var7) {
         throw new FSReadError(var7, this.filePath);
      }
   }

   public long size() throws FSReadError {
      try {
         return ((FileChannel)this.channel).size();
      } catch (IOException var2) {
         throw new FSReadError(var2, this.filePath);
      }
   }

   public void tryToSkipCache(long offset, long len) {
      NativeLibrary.trySkipCache(NativeLibrary.getfd((FileChannel)this.channel), offset, len, this.filePath);
   }
}
