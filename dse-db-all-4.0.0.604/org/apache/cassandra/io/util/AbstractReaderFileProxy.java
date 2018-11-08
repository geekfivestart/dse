package org.apache.cassandra.io.util;

public abstract class AbstractReaderFileProxy implements ReaderFileProxy {
   protected final AsynchronousChannelProxy channel;
   protected final long fileLength;
   protected final int sectorSize;

   protected AbstractReaderFileProxy(AsynchronousChannelProxy channel, long fileLength) {
      this.channel = channel;
      this.fileLength = fileLength >= 0L?fileLength:channel.size();
      this.sectorSize = FileUtils.MountPoint.mountPointForDirectory(channel.filePath).sectorSize;

      assert Integer.bitCount(this.sectorSize) == 1 : String.format("Sector size %d must be a power of two", new Object[]{Integer.valueOf(this.sectorSize)});

   }

   public AsynchronousChannelProxy channel() {
      return this.channel;
   }

   public long fileLength() {
      return this.fileLength;
   }

   public String toString() {
      return this.getClass().getSimpleName() + "(filePath='" + this.channel + "')";
   }

   public void close() {
   }

   public double getCrcCheckChance() {
      return 0.0D;
   }

   public int roundUpToBlockSize(int size) {
      return size + this.sectorSize - 1 & -this.sectorSize;
   }

   public long roundDownToBlockSize(long size) {
      return size & (long)(-this.sectorSize);
   }
}
