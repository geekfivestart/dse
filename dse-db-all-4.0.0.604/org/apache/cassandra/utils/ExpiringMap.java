package org.apache.cassandra.utils;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTimeoutTask;
import org.apache.cassandra.concurrent.TPCTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpiringMap<K, V> {
   private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);
   private volatile boolean shutdown;
   private final ConcurrentMap<K, ExpiringMap.ExpiringObject<K, V>> cache = new ConcurrentHashMap();
   private final long defaultExpiration;
   private final Consumer<ExpiringMap.ExpiringObject<K, V>> postExpireHook;

   public ExpiringMap(long defaultExpiration, Consumer<ExpiringMap.ExpiringObject<K, V>> postExpireHook) {
      if(defaultExpiration <= 0L) {
         throw new IllegalArgumentException("Argument specified must be a positive number");
      } else {
         this.defaultExpiration = defaultExpiration;
         this.postExpireHook = postExpireHook;
      }
   }

   public void reset() {
      this.shutdown = false;
      this.cache.forEach((k, v) -> {
         v.cancel();
      });
      this.cache.clear();
   }

   public boolean shutdownBlocking(long timeout) {
      this.shutdown = true;
      Uninterruptibles.sleepUninterruptibly(timeout, TimeUnit.MILLISECONDS);
      return this.cache.isEmpty();
   }

   public V put(K key, V value) {
      return this.put(key, value, this.defaultExpiration, TPC.bestTPCTimer());
   }

   public V put(K key, V value, long timeout) {
      return this.put(key, value, timeout, TPC.bestTPCTimer());
   }

   public V put(K key, V value, long timeout, TPCTimer timer) {
      if(this.shutdown) {
         if(TPC.isTPCThread()) {
            logger.debug("Received request after messaging service shutdown, ignoring it");
            return null;
         }

         Uninterruptibles.sleepUninterruptibly(9223372036854775807L, TimeUnit.NANOSECONDS);
      }

      ExpiringMap.ExpiringObject<K, V> current = new ExpiringMap.ExpiringObject(timer, key, value, timeout);
      ExpiringMap.ExpiringObject<K, V> previous = (ExpiringMap.ExpiringObject)this.cache.put(key, current);
      V previousValue = null;
      if(previous != null) {
         previousValue = previous.getValue();
         previous.cancel();
      }

      current.submit((kv) -> {
         ExpiringMap.ExpiringObject<K, V> removed = (ExpiringMap.ExpiringObject)this.cache.remove(kv.left);
         if(removed != null) {
            this.postExpireHook.accept(removed);
         }

      });
      return previousValue == null?null:previousValue;
   }

   public V get(K key) {
      ExpiringMap.ExpiringObject<K, V> expiring = (ExpiringMap.ExpiringObject)this.cache.get(key);
      return expiring != null?expiring.getValue():null;
   }

   public V remove(K key) {
      ExpiringMap.ExpiringObject<K, V> removed = (ExpiringMap.ExpiringObject)this.cache.remove(key);
      V value = null;
      if(removed != null) {
         value = removed.getValue();
         removed.cancel();
      }

      return value;
   }

   public int size() {
      return this.cache.size();
   }

   public boolean containsKey(K key) {
      return this.cache.containsKey(key);
   }

   public boolean isEmpty() {
      return this.cache.isEmpty();
   }

   public Set<K> keySet() {
      return this.cache.keySet();
   }

   public static class ExpiringObject<K, V> {
      private final TPCTimeoutTask<Pair<K, V>> task;
      private final long timeout;

      private ExpiringObject(TPCTimer timer, K key, V value, long timeout) {
         this.task = new TPCTimeoutTask(timer, Pair.create(key, value));
         this.timeout = timeout;
      }

      public K getKey() {
         Pair<K, V> kv = (Pair)this.task.getValue();
         return kv != null?kv.left:null;
      }

      public V getValue() {
         Pair<K, V> kv = (Pair)this.task.getValue();
         return kv != null?kv.right:null;
      }

      public long timeoutMillis() {
         return this.timeout;
      }

      void submit(Consumer<Pair<K, V>> action) {
         this.task.submit(action, this.timeout, TimeUnit.MILLISECONDS);
      }

      void cancel() {
         this.task.dispose();
      }
   }
}
