package org.apache.cassandra.service;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum RebuildMode {
   NORMAL {
      public void beforeStreaming(List<String> keyspaces) {
      }

      public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces) {
      }
   },
   REFETCH {
      public void beforeStreaming(List<String> keyspaces) {
         TPCUtils.blockingAwait(CompletableFuture.allOf((CompletableFuture[])keyspaces.stream().peek((ks) -> {
            RebuildMode.logger.info("Resetting available ranges for keyspace {}", ks);
         }).map(SystemKeyspace::resetAvailableRanges).toArray((x$0) -> {
            return new CompletableFuture[x$0];
         })));
      }

      public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces) {
         TPCUtils.blockingAwait(CompletableFuture.allOf((CompletableFuture[])rangesPerKeyspaces.entrySet().stream().peek((entry) -> {
            RebuildMode.logger.info("Resetting available ranges for keyspace {}: {}", entry.getKey(), entry.getValue());
         }).map((entry) -> {
            return SystemKeyspace.resetAvailableRanges((String)entry.getKey(), (Collection)entry.getValue());
         }).toArray((x$0) -> {
            return new CompletableFuture[x$0];
         })));
      }
   },
   RESET {
      public void beforeStreaming(List<String> keyspaces) {
         RebuildMode.resetAndTruncate(keyspaces, DatabaseDescriptor.isAutoSnapshot());
      }

      public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces) {
         throw new IllegalArgumentException("mode=reset is only supported for all ranges");
      }
   },
   RESET_NO_SNAPSHOT {
      public void beforeStreaming(List<String> keyspaces) {
         RebuildMode.resetAndTruncate(keyspaces, false);
      }

      public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces) {
         throw new IllegalArgumentException("mode=reset-no-snapshot is only supported for all ranges");
      }
   };

   private static final Logger logger = LoggerFactory.getLogger(RebuildMode.class);

   private RebuildMode() {
   }

   public abstract void beforeStreaming(List<String> var1);

   public abstract void beforeStreaming(Map<String, Collection<Range<Token>>> var1);

   public static RebuildMode getMode(String name) {
      if(name == null) {
         return NORMAL;
      } else {
         String modeName = name.toUpperCase(Locale.US).replaceAll("-", "_");
         RebuildMode[] var2 = values();
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            RebuildMode mode = var2[var4];
            if(mode.name().equals(modeName)) {
               return mode;
            }
         }

         throw new IllegalArgumentException("Unknown mode used for rebuild: " + name);
      }
   }

   private static void resetAndTruncate(List<String> keyspaces, boolean snapshot) {
      Iterator var2 = keyspaces.iterator();

      while(var2.hasNext()) {
         String keyspaceName = (String)var2.next();
         logger.info("Resetting available ranges for keyspace {}", keyspaceName);
         TPCUtils.blockingAwait(SystemKeyspace.resetAvailableRanges(keyspaceName));
         Keyspace.open(keyspaceName).getColumnFamilyStores().forEach((cfs) -> {
            logger.info("Truncating table {}.{}{}", new Object[]{keyspaceName, cfs.name, snapshot?", with snapshot":", no snapshot"});
            cfs.truncateBlocking(snapshot);
         });
      }

   }
}
