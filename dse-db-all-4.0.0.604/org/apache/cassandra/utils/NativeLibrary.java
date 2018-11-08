package org.apache.cassandra.utils;

import com.sun.jna.LastErrorException;
import io.netty.channel.epoll.AIOEpollFileChannel;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

public final class NativeLibrary {
   private static final Logger logger = LoggerFactory.getLogger(NativeLibrary.class);
   private static final NativeLibrary.OSType osType = getOsType();
   private static final int MCL_CURRENT;
   private static final int MCL_FUTURE;
   private static final int ENOMEM = 12;
   private static final int F_GETFL = 3;
   private static final int F_SETFL = 4;
   private static final int F_NOCACHE = 48;
   private static final int O_DIRECT = 16384;
   private static final int O_RDONLY = 0;
   private static final int POSIX_FADV_NORMAL = 0;
   private static final int POSIX_FADV_RANDOM = 1;
   private static final int POSIX_FADV_SEQUENTIAL = 2;
   private static final int POSIX_FADV_WILLNEED = 3;
   private static final int POSIX_FADV_DONTNEED = 4;
   private static final int POSIX_FADV_NOREUSE = 5;
   private static final NativeLibraryWrapper wrappedLibrary;
   private static boolean jnaLockable = false;
   private static final Field FILE_DESCRIPTOR_FD_FIELD = FBUtilities.getProtectedField(FileDescriptor.class, "fd");
   private static final Field FILE_CHANNEL_FD_FIELD = FBUtilities.getProtectedField(FileChannelImpl.class, "fd");
   private static final Field FILE_ASYNC_CHANNEL_FD_FIELD = FBUtilities.getProtectedField("sun.nio.ch.AsynchronousFileChannelImpl", "fdObj");

   private NativeLibrary() {
   }

   private static NativeLibrary.OSType getOsType() {
      String osName = System.getProperty("os.name").toLowerCase();
      if(osName.contains("linux")) {
         return NativeLibrary.OSType.LINUX;
      } else if(osName.contains("mac")) {
         return NativeLibrary.OSType.MAC;
      } else if(osName.contains("windows")) {
         return NativeLibrary.OSType.WINDOWS;
      } else {
         logger.warn("the current operating system, {}, is unsupported by cassandra", osName);
         return osName.contains("aix")?NativeLibrary.OSType.AIX:NativeLibrary.OSType.LINUX;
      }
   }

   private static int errno(RuntimeException e) {
      assert e instanceof LastErrorException;

      try {
         return ((LastErrorException)e).getErrorCode();
      } catch (NoSuchMethodError var2) {
         logger.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
         return 0;
      }
   }

   public static boolean isAvailable() {
      return wrappedLibrary.isAvailable();
   }

   public static boolean jnaMemoryLockable() {
      return jnaLockable;
   }

   public static void tryMlockall() {
      try {
         wrappedLibrary.callMlockall(MCL_CURRENT);
         jnaLockable = true;
         logger.info("JNA mlockall successful");
      } catch (UnsatisfiedLinkError var1) {
         ;
      } catch (RuntimeException var2) {
         if(!(var2 instanceof LastErrorException)) {
            throw var2;
         }

         if(errno(var2) == 12 && osType == NativeLibrary.OSType.LINUX) {
            logger.warn("Unable to lock JVM memory (ENOMEM). This can result in part of the JVM being swapped out, especially with mmapped I/O enabled. Increase RLIMIT_MEMLOCK or run Cassandra as root.");
         } else if(osType != NativeLibrary.OSType.MAC) {
            logger.warn("Unknown mlockall error {}", Integer.valueOf(errno(var2)));
         }
      }

   }

   public static void trySkipCache(String path, long offset, long len) {
      File f = new File(path);
      if(f.exists()) {
         try {
            SeekableByteChannel fc = Files.newByteChannel(f.toPath(), new OpenOption[0]);
            Throwable var7 = null;

            try {
               trySkipCache(getfd((FileChannel)fc), offset, len, path);
            } catch (Throwable var17) {
               var7 = var17;
               throw var17;
            } finally {
               if(fc != null) {
                  if(var7 != null) {
                     try {
                        fc.close();
                     } catch (Throwable var16) {
                        var7.addSuppressed(var16);
                     }
                  } else {
                     fc.close();
                  }
               }

            }
         } catch (IOException var19) {
            logger.warn("Could not skip cache", var19);
         }

      }
   }

   public static void trySkipCache(int fd, long offset, long len, String path) {
      if(len == 0L) {
         trySkipCache(fd, 0L, 0, path);
      }

      while(len > 0L) {
         int sublen = (int)Math.min(2147483647L, len);
         trySkipCache(fd, offset, sublen, path);
         len -= (long)sublen;
         offset -= (long)sublen;
      }

   }

   public static void trySkipCache(int fd, long offset, int len, String path) {
      if(fd >= 0) {
         try {
            if(osType == NativeLibrary.OSType.LINUX) {
               int result = wrappedLibrary.callPosixFadvise(fd, offset, len, 4);
               if(result != 0) {
                  NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 10L, TimeUnit.MINUTES, "Failed trySkipCache on file: {} Error: " + wrappedLibrary.callStrerror(result).getString(0L), new Object[]{path});
               }
            }
         } catch (UnsatisfiedLinkError var6) {
            ;
         } catch (RuntimeException var7) {
            if(!(var7 instanceof LastErrorException)) {
               throw var7;
            }

            logger.warn("posix_fadvise({}, {}) failed, errno ({}).", new Object[]{Integer.valueOf(fd), Long.valueOf(offset), Integer.valueOf(errno(var7))});
         }

      }
   }

   public static int tryFcntl(int fd, int command, int flags) {
      int result = -1;

      try {
         result = wrappedLibrary.callFcntl(fd, command, (long)flags);
      } catch (UnsatisfiedLinkError var5) {
         ;
      } catch (RuntimeException var6) {
         if(!(var6 instanceof LastErrorException)) {
            throw var6;
         }

         logger.warn("fcntl({}, {}, {}) failed, errno ({}).", new Object[]{Integer.valueOf(fd), Integer.valueOf(command), Integer.valueOf(flags), Integer.valueOf(errno(var6))});
      }

      return result;
   }

   public static int tryOpenDirectory(String path) {
      byte fd = -1;

      try {
         return wrappedLibrary.callOpen(path, 0);
      } catch (UnsatisfiedLinkError var3) {
         ;
      } catch (RuntimeException var4) {
         if(!(var4 instanceof LastErrorException)) {
            throw var4;
         }

         logger.warn("open({}, O_RDONLY) failed, errno ({}).", path, Integer.valueOf(errno(var4)));
      }

      return fd;
   }

   public static void trySync(int fd) {
      if(fd != -1) {
         try {
            wrappedLibrary.callFsync(fd);
         } catch (UnsatisfiedLinkError var2) {
            ;
         } catch (RuntimeException var3) {
            if(!(var3 instanceof LastErrorException)) {
               throw var3;
            }

            logger.warn("fsync({}) failed, errorno ({}) {}", new Object[]{Integer.valueOf(fd), Integer.valueOf(errno(var3)), var3.getMessage()});
         }

      }
   }

   public static void tryCloseFD(int fd) {
      if(fd != -1) {
         try {
            wrappedLibrary.callClose(fd);
         } catch (UnsatisfiedLinkError var2) {
            ;
         } catch (RuntimeException var3) {
            if(!(var3 instanceof LastErrorException)) {
               throw var3;
            }

            logger.warn("close({}) failed, errno ({}).", Integer.valueOf(fd), Integer.valueOf(errno(var3)));
         }

      }
   }

   public static int getfd(AsynchronousFileChannel channel) {
      try {
         return channel instanceof AIOEpollFileChannel?((AIOEpollFileChannel)channel).getFd():getfd((FileDescriptor)FILE_ASYNC_CHANNEL_FD_FIELD.get(channel));
      } catch (IllegalAccessException | IllegalArgumentException var2) {
         logger.warn("Unable to read fd field from FileChannel");
         return -1;
      }
   }

   public static int getfd(FileChannel channel) {
      try {
         return getfd(getFileDescriptor(channel));
      } catch (IllegalArgumentException var2) {
         throw new RuntimeException("Unable to read fd field from FileChannel", var2);
      }
   }

   public static FileDescriptor getFileDescriptor(FileChannel channel) {
      try {
         return (FileDescriptor)FILE_CHANNEL_FD_FIELD.get(channel);
      } catch (IllegalAccessException | IllegalArgumentException var2) {
         throw new RuntimeException(var2);
      }
   }

   public static int getfd(FileDescriptor descriptor) {
      try {
         return FILE_DESCRIPTOR_FD_FIELD.getInt(descriptor);
      } catch (Exception var2) {
         JVMStabilityInspector.inspectThrowable(var2);
         logger.warn("Unable to read fd field from FileDescriptor");
         return -1;
      }
   }

   public static long getProcessID() {
      try {
         return wrappedLibrary.callGetpid();
      } catch (Exception var1) {
         logger.info("Failed to get PID from JNA", var1);
         return -1L;
      }
   }

   public static boolean tryMlock(long address, long length) {
      try {
         int result = wrappedLibrary.callMlock(address, length);
         logger.debug("JNA mlock successful result: {} address: {} length: {}", new Object[]{Integer.valueOf(result), Long.valueOf(address), Long.valueOf(length)});
         return true;
      } catch (UnsatisfiedLinkError var6) {
         logger.debug("mlock failed: " + var6.getMessage());
         return false;
      } catch (RuntimeException var7) {
         if(!(var7 instanceof LastErrorException)) {
            throw var7;
         } else {
            int errNum = errno(var7);
            if(errNum == 12) {
               logger.warn("Unable to lock JVM memory (ENOMEM) at address: {} length: {}. You may need to increase RLIMIT_MEMLOCK for the dse user.", Long.valueOf(address), Long.valueOf(length));
               logger.debug("Exception: ", var7);
            } else {
               logger.warn("Unknown mlock error: {} msg: {}", Integer.valueOf(errNum), wrappedLibrary.callStrerror(errNum));
               logger.debug("Exception: ", var7);
            }

            return false;
         }
      }
   }

   static {
      switch(null.$SwitchMap$org$apache$cassandra$utils$NativeLibrary$OSType[osType.ordinal()]) {
      case 1:
         wrappedLibrary = new NativeLibraryDarwin();
         break;
      case 2:
         wrappedLibrary = new NativeLibraryWindows();
         break;
      case 3:
      case 4:
      case 5:
      default:
         wrappedLibrary = new NativeLibraryLinux();
      }

      if(System.getProperty("os.arch").toLowerCase().contains("ppc")) {
         if(osType == NativeLibrary.OSType.LINUX) {
            MCL_CURRENT = 8192;
            MCL_FUTURE = 16384;
         } else if(osType == NativeLibrary.OSType.AIX) {
            MCL_CURRENT = 256;
            MCL_FUTURE = 512;
         } else {
            MCL_CURRENT = 1;
            MCL_FUTURE = 2;
         }
      } else {
         MCL_CURRENT = 1;
         MCL_FUTURE = 2;
      }

   }

   public static enum OSType {
      LINUX,
      MAC,
      WINDOWS,
      AIX,
      OTHER;

      private OSType() {
      }
   }
}
