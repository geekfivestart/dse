package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JVMStabilityInspector {
   private static final Logger logger = LoggerFactory.getLogger(JVMStabilityInspector.class);
   private static JVMKiller killer = new JVMStabilityInspector.Killer();
   private static final List<Pair<Thread, Runnable>> shutdownHooks = new ArrayList(1);
   private static AtomicBoolean printingHeapHistogram = new AtomicBoolean(false);
   private static volatile ErrorHandler diskHandler;
   private static ErrorHandler globalHandler;

   private JVMStabilityInspector() {
   }

   public static void setDiskErrorHandler(ErrorHandler errorHandler) {
      diskHandler = errorHandler;
   }

   public static void inspectThrowable(Throwable error) {
      inspectThrowable(error, new ErrorHandler[]{diskHandler, globalHandler});
   }

   public static void inspectThrowable(Throwable error, @Nullable ErrorHandler handler) {
      inspectThrowable(error, new ErrorHandler[]{handler, globalHandler});
   }

   public static void inspectThrowable(Throwable error, ErrorHandler... handlers) {
      while(error != null) {
         ErrorHandler[] var2 = handlers;
         int var3 = handlers.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            ErrorHandler handler = var2[var4];
            if(handler != null) {
               handler.handleError(error);
            }
         }

         error = error.getCause();
      }

   }

   private static boolean printHeapHistogramOnOutOfMemoryError() {
      String property = System.getProperty("cassandra.printHeapHistogramOnOutOfMemoryError");
      return property == null?true:Boolean.parseBoolean(property);
   }

   public static void killJVM(Throwable t, boolean quiet) {
      killer.killJVM(t, quiet);
   }


   public static void userFunctionTimeout(Throwable t) {
      switch (DatabaseDescriptor.getUserFunctionTimeoutPolicy()) {
         case die: {
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> killer.killJVM(t), 250L, TimeUnit.MILLISECONDS);
            break;
         }
         case die_immediate: {
            killer.killJVM(t);
            break;
         }
         case ignore: {
            logger.error(t.getMessage());
         }
      }
   }

   public static void registerShutdownHook(Thread hook, Runnable runOnHookRemoved) {
      Runtime.getRuntime().addShutdownHook(hook);
      shutdownHooks.add(Pair.create(hook, runOnHookRemoved));
   }

   @VisibleForTesting
   public static JVMKiller replaceKiller(JVMKiller newKiller) {
      JVMKiller oldKiller = killer;
      killer = newKiller;
      return oldKiller;
   }

   public static JVMKiller killer() {
      return killer;
   }

   public static void removeShutdownHooks() {
      Throwable err = null;

      Throwables.DiscreteAction[] var10001;
      for(Iterator var1 = shutdownHooks.iterator(); var1.hasNext(); err = Throwables.perform(err, var10001)) {
         Pair<Thread, Runnable> hook = (Pair)var1.next();
         var10001 = new Throwables.DiscreteAction[]{() -> {
            Runtime.getRuntime().removeShutdownHook((Thread)hook.left);
         }, null};
         Runnable var10004 = (Runnable)hook.right;
         ((Runnable)hook.right).getClass();
         var10001[1] = var10004::run;
      }

      if(err != null) {
         logger.error("Got error when removing shutdown hook(s): {}", err.getMessage(), err);
      }

      shutdownHooks.clear();
   }

   static {
      diskHandler = new StartupDiskErrorHandler(killer);
      globalHandler = new JVMStabilityInspector.GlobalHandler();
   }

   private static class Killer implements JVMKiller {
      private final AtomicBoolean killing;

      private Killer() {
         this.killing = new AtomicBoolean();
      }

      public void killJVM(Throwable t, boolean quiet) {
         if(!quiet) {
            t.printStackTrace(System.err);
            JVMStabilityInspector.logger.error("JVM state determined to be unstable.  Exiting forcefully due to:", t);
         }

         if(this.killing.compareAndSet(false, true)) {
            JVMStabilityInspector.removeShutdownHooks();
            System.exit(100);
         }

      }
   }

   private static class GlobalHandler implements ErrorHandler {
      private GlobalHandler() {
      }

      public void handleError(Throwable t) {
         boolean isUnstable = false;
         if(JVMStabilityInspector.logger.isTraceEnabled()) {
            JVMStabilityInspector.logger.trace("Inspecting {}/{}", new Object[]{t.getClass(), t.getMessage(), t});
         }

         if(t instanceof OutOfMemoryError) {
            if(JVMStabilityInspector.printHeapHistogramOnOutOfMemoryError()) {
               if(!JVMStabilityInspector.printingHeapHistogram.compareAndSet(false, true)) {
                  return;
               }

               HeapUtils.logHeapHistogram();
            }

            JVMStabilityInspector.logger.error("OutOfMemory error letting the JVM handle the error:", t);
            JVMStabilityInspector.removeShutdownHooks();
            throw (OutOfMemoryError)t;
         } else {
            if(t instanceof LinkageError) {
               isUnstable = true;
            }

            if((t instanceof FileNotFoundException || t instanceof SocketException) && t.getMessage().contains("Too many open files")) {
               isUnstable = true;
            }

            if(isUnstable) {
               JVMStabilityInspector.killer.killJVM(t);
            }

         }
      }
   }
}
