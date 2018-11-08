package org.apache.cassandra.utils;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncUtil {
   public static boolean SKIP_SYNC = Boolean.getBoolean("cassandra.skip_sync");
   private static final Field mbbFDField;
   private static final Field fdClosedField;
   private static final Field fdUseCountField;

   public SyncUtil() {
   }

   public static MappedByteBuffer force(MappedByteBuffer buf) {
      Objects.requireNonNull(buf);
      if(SKIP_SYNC) {
         Object fd = null;

         try {
            if(mbbFDField != null) {
               fd = mbbFDField.get(buf);
            }
         } catch (Exception var3) {
            throw new RuntimeException(var3);
         }

         if(mbbFDField != null && fd == null) {
            throw new UnsupportedOperationException();
         } else {
            return buf;
         }
      } else {
         return buf.force();
      }
   }

   public static void sync(FileDescriptor fd) throws SyncFailedException {
      Objects.requireNonNull(fd);
      if(SKIP_SYNC) {
         boolean closed = false;

         try {
            if(fdClosedField != null) {
               closed = fdClosedField.getBoolean(fd);
            }
         } catch (Exception var5) {
            throw new RuntimeException(var5);
         }

         int useCount = 1;

         try {
            if(fdUseCountField != null) {
               useCount = ((AtomicInteger)fdUseCountField.get(fd)).get();
            }
         } catch (Exception var4) {
            throw new RuntimeException(var4);
         }

         if(closed || !fd.valid() || useCount < 0) {
            throw new SyncFailedException("Closed " + closed + " valid " + fd.valid() + " useCount " + useCount);
         }
      } else {
         fd.sync();
      }

   }

   public static void force(FileChannel fc, boolean metaData) throws IOException {
      Objects.requireNonNull(fc);
      if(SKIP_SYNC) {
         if(!fc.isOpen()) {
            throw new ClosedChannelException();
         }
      } else {
         fc.force(metaData);
      }

   }

   public static void forceAlways(FileChannel fc, boolean metaData) throws IOException {
      Objects.requireNonNull(fc);
      fc.force(metaData);
   }

   public static void sync(RandomAccessFile ras) throws IOException {
      Objects.requireNonNull(ras);
      sync(ras.getFD());
   }

   public static void sync(FileOutputStream fos) throws IOException {
      Objects.requireNonNull(fos);
      sync(fos.getFD());
   }

   public static void sync(FileChannel fc) throws IOException {
      Objects.requireNonNull(fc);
      sync(NativeLibrary.getFileDescriptor(fc));
   }

   public static void trySync(int fd) {
      if(!SKIP_SYNC) {
         NativeLibrary.trySync(fd);
      }
   }

   public static void trySyncDir(File dir) {
      if(!SKIP_SYNC) {
         int directoryFD = NativeLibrary.tryOpenDirectory(dir.getPath());

         try {
            trySync(directoryFD);
         } finally {
            NativeLibrary.tryCloseFD(directoryFD);
         }

      }
   }

   static {
      Field mbbFDFieldTemp = null;

      try {
         mbbFDFieldTemp = MappedByteBuffer.class.getDeclaredField("fd");
         mbbFDFieldTemp.setAccessible(true);
      } catch (NoSuchFieldException var6) {
         ;
      }

      mbbFDField = mbbFDFieldTemp;
      Field fdClosedFieldTemp = null;

      try {
         fdClosedFieldTemp = FileDescriptor.class.getDeclaredField("closed");
         fdClosedFieldTemp.setAccessible(true);
      } catch (NoSuchFieldException var5) {
         ;
      }

      fdClosedField = fdClosedFieldTemp;
      Field fdUseCountTemp = null;

      try {
         fdUseCountTemp = FileDescriptor.class.getDeclaredField("useCount");
         fdUseCountTemp.setAccessible(true);
      } catch (NoSuchFieldException var4) {
         ;
      }

      fdUseCountField = fdUseCountTemp;
   }
}
