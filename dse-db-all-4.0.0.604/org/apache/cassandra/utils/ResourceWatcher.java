package org.apache.cassandra.utils;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceWatcher {
   public ResourceWatcher() {
   }

   public static void watch(String resource, Runnable callback, int period) {
      ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(new ResourceWatcher.WatchedResource(resource, callback), (long)period, (long)period, TimeUnit.MILLISECONDS);
   }

   public static class WatchedResource implements Runnable {
      private static final Logger logger = LoggerFactory.getLogger(ResourceWatcher.WatchedResource.class);
      private final String resource;
      private final Runnable callback;
      private long lastLoaded;

      public WatchedResource(String resource, Runnable callback) {
         this.resource = resource;
         this.callback = callback;
         this.lastLoaded = 0L;
      }

      public void run() {
         try {
            String filename = FBUtilities.resourceToFile(this.resource);
            long lastModified = (new File(filename)).lastModified();
            if(lastModified > this.lastLoaded) {
               this.callback.run();
               this.lastLoaded = lastModified;
            }
         } catch (Throwable var4) {
            JVMStabilityInspector.inspectThrowable(var4);
            logger.error(String.format("Timed run of %s failed.", new Object[]{this.callback.getClass()}), var4);
         }

      }
   }
}
