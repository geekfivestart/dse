package org.apache.cassandra.utils;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeLibraryWindows implements NativeLibraryWrapper {
   private static final Logger logger = LoggerFactory.getLogger(NativeLibraryWindows.class);
   private static boolean available;

   public NativeLibraryWindows() {
   }

   private static native long GetCurrentProcessId() throws LastErrorException;

   public int callMlockall(int flags) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public int callMunlockall() throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public int callFcntl(int fd, int command, long flags) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public int callPosixFadvise(int fd, long offset, int len, int flag) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public int callOpen(String path, int flags) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public int callFsync(int fd) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public int callClose(int fd) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public Pointer callStrerror(int errnum) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public int callMlock(long address, long length) throws UnsatisfiedLinkError, RuntimeException {
      throw new UnsatisfiedLinkError();
   }

   public long callGetpid() throws UnsatisfiedLinkError, RuntimeException {
      return GetCurrentProcessId();
   }

   public boolean isAvailable() {
      return available;
   }

   static {
      try {
         Native.register("kernel32");
         available = true;
      } catch (NoClassDefFoundError var1) {
         logger.warn("JNA not found. Native methods will be disabled.");
      } catch (UnsatisfiedLinkError var2) {
         logger.error("Failed to link the Windows/Kernel32 library against JNA. Native methods will be unavailable.", var2);
      } catch (NoSuchMethodError var3) {
         logger.warn("Obsolete version of JNA present; unable to register Windows/Kernel32 library. Upgrade to JNA 3.2.7 or later");
      }

   }
}
