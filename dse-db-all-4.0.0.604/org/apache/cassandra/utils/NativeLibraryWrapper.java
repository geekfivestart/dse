package org.apache.cassandra.utils;

import com.sun.jna.Pointer;

interface NativeLibraryWrapper {
   boolean isAvailable();

   int callMlockall(int var1) throws UnsatisfiedLinkError, RuntimeException;

   int callMunlockall() throws UnsatisfiedLinkError, RuntimeException;

   int callFcntl(int var1, int var2, long var3) throws UnsatisfiedLinkError, RuntimeException;

   int callPosixFadvise(int var1, long var2, int var4, int var5) throws UnsatisfiedLinkError, RuntimeException;

   int callOpen(String var1, int var2) throws UnsatisfiedLinkError, RuntimeException;

   int callFsync(int var1) throws UnsatisfiedLinkError, RuntimeException;

   int callClose(int var1) throws UnsatisfiedLinkError, RuntimeException;

   Pointer callStrerror(int var1) throws UnsatisfiedLinkError, RuntimeException;

   long callGetpid() throws UnsatisfiedLinkError, RuntimeException;

   int callMlock(long var1, long var3) throws UnsatisfiedLinkError, RuntimeException;
}
