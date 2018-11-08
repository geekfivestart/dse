package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
   public final String id;
   private final int priority;
   private final ClassLoader contextClassLoader;
   private final ThreadGroup threadGroup;
   protected final AtomicInteger n;
   private static final AtomicInteger threadCounter = new AtomicInteger();

   public NamedThreadFactory(String id) {
      this(id, 5);
   }

   public NamedThreadFactory(String id, int priority) {
      this(id, priority, (ClassLoader)null, (ThreadGroup)null);
   }

   public NamedThreadFactory(String id, int priority, ClassLoader contextClassLoader, ThreadGroup threadGroup) {
      this.n = new AtomicInteger(1);
      this.id = id;
      this.priority = priority;
      this.contextClassLoader = contextClassLoader;
      this.threadGroup = threadGroup;
   }

   public Thread newThread(Runnable runnable) {
      String name = this.id + ':' + this.n.getAndIncrement();
      Thread thread = createThread(this.threadGroup, runnable, name, true);
      thread.setPriority(this.priority);
      if(this.contextClassLoader != null) {
         thread.setContextClassLoader(this.contextClassLoader);
      }

      return thread;
   }

   public static Runnable threadLocalDeallocator(Runnable r) {
      return () -> {
         try {
            r.run();
         } finally {
            FastThreadLocal.removeAll();
         }

      };
   }

   @VisibleForTesting
   public static Thread createThread(Runnable runnable) {
      return createThread((ThreadGroup)null, runnable, "anonymous-" + threadCounter.incrementAndGet());
   }

   public static Thread createThread(Runnable runnable, String name) {
      return createThread((ThreadGroup)null, runnable, name);
   }

   public static Thread createThread(Runnable runnable, String name, boolean daemon) {
      return createThread((ThreadGroup)null, runnable, name, daemon);
   }

   public static Thread createThread(ThreadGroup threadGroup, Runnable runnable, String name) {
      return createThread(threadGroup, runnable, name, false);
   }

   public static Thread createThread(ThreadGroup threadGroup, Runnable runnable, String name, boolean daemon) {
      Thread thread = new FastThreadLocalThread(threadGroup, threadLocalDeallocator(runnable), name);
      thread.setDaemon(daemon);
      return thread;
   }
}
