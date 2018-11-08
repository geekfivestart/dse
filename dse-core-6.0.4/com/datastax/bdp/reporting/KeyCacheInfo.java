package com.datastax.bdp.reporting;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.io.sstable.format.big.BigRowIndexEntry;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;

public class KeyCacheInfo extends PersistedSystemInfo {
   public static final String KEYCACHE_INSERT = String.format("INSERT INTO %s.%s (node_ip,cache_size,cache_capacity,cache_hits,cache_requests,hit_rate)VALUES (?,?,?,?,?,?) USING TTL ?", new Object[]{"dse_perf", "key_cache"});
   private final AutoSavingCache<KeyCacheKey, BigRowIndexEntry> keyCache;

   public KeyCacheInfo(InetAddress nodeAddress, int ttl, AutoSavingCache<KeyCacheKey, BigRowIndexEntry> keyCache) {
      super(nodeAddress, ttl);
      this.keyCache = keyCache;
   }

   protected String getTableName() {
      return "key_cache";
   }

   protected String getInsertCQL() {
      return KEYCACHE_INSERT;
   }

   protected List<ByteBuffer> getVariables() {
      CacheMetrics metrics = this.keyCache.getMetrics();
      List<ByteBuffer> vars = new ArrayList();
      vars.add(this.nodeAddressBytes);
      vars.add(ByteBufferUtil.bytes(((Long)metrics.size.getValue()).longValue()));
      vars.add(ByteBufferUtil.bytes(((Long)metrics.capacity.getValue()).longValue()));
      vars.add(ByteBufferUtil.bytes(metrics.hits.getCount()));
      vars.add(ByteBufferUtil.bytes(metrics.requests.getCount()));
      vars.add(ByteBufferUtil.bytes(((Double)metrics.hitRate.getValue()).doubleValue()));
      vars.add(this.getTtlBytes());
      return vars;
   }
}
