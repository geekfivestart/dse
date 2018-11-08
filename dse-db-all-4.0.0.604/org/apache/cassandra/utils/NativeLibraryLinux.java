package org.apache.cassandra.utils;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeLibraryLinux implements NativeLibraryWrapper {
   private static boolean available;
   private static final Logger logger = LoggerFactory.getLogger(NativeLibraryLinux.class);

   public NativeLibraryLinux() {
   }

   private static native int mlockall(int var0) throws LastErrorException;

   private static native int munlockall() throws LastErrorException;

   private static native int fcntl(int var0, int var1, long var2) throws LastErrorException;

   private static native int posix_fadvise(int var0, long var1, int var3, int var4) throws LastErrorException;

   private static native int open(String var0, int var1) throws LastErrorException;

   private static native int fsync(int var0) throws LastErrorException;

   private static native int close(int var0) throws LastErrorException;

   private static native Pointer strerror(int var0) throws LastErrorException;

   private static native long getpid() throws LastErrorException;

   private static native int mlock(Pointer var0, NativeLong var1) throws LastErrorException;

   public int callMlockall(int flags) throws UnsatisfiedLinkError, RuntimeException {
      return mlockall(flags);
   }

   public int callMunlockall() throws UnsatisfiedLinkError, RuntimeException {
      return munlockall();
   }

   public int callFcntl(int fd, int command, long flags) throws UnsatisfiedLinkError, RuntimeException {
      return fcntl(fd, command, flags);
   }

   public int callPosixFadvise(int fd, long offset, int len, int flag) throws UnsatisfiedLinkError, RuntimeException {
      return posix_fadvise(fd, offset, len, flag);
   }

   public int callOpen(String path, int flags) throws UnsatisfiedLinkError, RuntimeException {
      return open(path, flags);
   }

   public int callFsync(int fd) throws UnsatisfiedLinkError, RuntimeException {
      return fsync(fd);
   }

   public int callClose(int fd) throws UnsatisfiedLinkError, RuntimeException {
      return close(fd);
   }

   public Pointer callStrerror(int errnum) throws UnsatisfiedLinkError, RuntimeException {
      return strerror(errnum);
   }

   public long callGetpid() throws UnsatisfiedLinkError, RuntimeException {
      return getpid();
   }

   public int callMlock(long address, long length) throws UnsatisfiedLinkError, RuntimeException {
      return mlock(new Pointer(address), new NativeLong(length));
   }

   public boolean isAvailable() {
      return available;
   }

   static {
      try {
         Native.register("c");
         available = true;
      } catch (NoClassDefFoundError var1) {
         logger.warn("JNA not found. Native methods will be disabled.");
      } catch (UnsatisfiedLinkError var2) {
         logger.error("Failed to link the C library against JNA. Native methods will be unavailable.", var2);
      } catch (NoSuchMethodError var3) {
         logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
      }

   }
}
