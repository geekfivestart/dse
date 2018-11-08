package org.apache.cassandra.config;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class ContinuousPagingConfig {
   private static final int DEFAULT_MAX_CONCURRENT_SESSIONS = 60;
   private static final int DEFAULT_MAX_SESSION_PAGES = 4;
   private static final int DEFAULT_MAX_PAGE_SIZE_MB = 8;
   private static final int DEFAULT_MAX_LOCAL_QUERY_TIME_MS = 5000;
   private static final int DEFAULT_CLIENT_TIMEOUT_SEC = 120;
   private static final int DEFAULT_CANCEL_TIMEOUT_SEC = 5;
   private static final int DEFAULT_PAUSED_CHECK_INTERVAL_MS = 1;
   public int max_concurrent_sessions;
   public int max_session_pages;
   public int max_page_size_mb;
   public int max_local_query_time_ms;
   public int client_timeout_sec;
   public int cancel_timeout_sec;
   public int paused_check_interval_ms;
   public List<Integer> max_sessions_per_core;

   public ContinuousPagingConfig() {
      this(60, 4, 8, 5000, 120, 5, 1);
   }

   public ContinuousPagingConfig(int max_concurrent_sessions, int max_session_pages, int max_page_size_mb, int max_local_query_time_ms, int client_timeout_sec, int cancel_timeout_sec, int paused_check_interval_ms) {
      this.max_concurrent_sessions = max_concurrent_sessions;
      this.max_session_pages = max_session_pages;
      this.max_page_size_mb = max_page_size_mb;
      this.max_local_query_time_ms = max_local_query_time_ms;
      this.client_timeout_sec = client_timeout_sec;
      this.cancel_timeout_sec = cancel_timeout_sec;
      this.paused_check_interval_ms = paused_check_interval_ms;
      this.max_sessions_per_core = new ArrayList();
   }

   private synchronized void setMaxConcurrentSessionsPerCore(int numCores) {
      assert numCores >= 1 : "invalid number of cores, should be positive";

      if(this.max_sessions_per_core.size() < numCores) {
         this.max_sessions_per_core.clear();
         int val = this.max_concurrent_sessions / numCores;
         int remainder = this.max_concurrent_sessions % numCores;

         for(int i = 0; i < numCores; ++i) {
            this.max_sessions_per_core.add(Integer.valueOf(val + (i < remainder?1:0)));
         }

      }
   }

   public List<Integer> getMaxSessionsCoreDistribution(int numCores) {
      if(this.max_sessions_per_core.size() < numCores) {
         this.setMaxConcurrentSessionsPerCore(numCores);
      }

      return this.max_sessions_per_core;
   }

   public int getMaxSessionsPerCore(int numCores, int coreId) {
      if(this.max_sessions_per_core.size() < numCores) {
         this.setMaxConcurrentSessionsPerCore(numCores);
      }

      assert coreId >= 0 && coreId <= this.max_sessions_per_core.size() : "invalid core id";

      return ((Integer)this.max_sessions_per_core.get(coreId)).intValue();
   }

   public boolean equals(Object other) {
      if(this == other) {
         return true;
      } else if(!(other instanceof ContinuousPagingConfig)) {
         return false;
      } else {
         ContinuousPagingConfig that = (ContinuousPagingConfig)other;
         return this.max_concurrent_sessions == that.max_concurrent_sessions && this.max_session_pages == that.max_session_pages && this.max_page_size_mb == that.max_page_size_mb && this.max_local_query_time_ms == that.max_local_query_time_ms && this.client_timeout_sec == that.client_timeout_sec && this.cancel_timeout_sec == that.cancel_timeout_sec && this.paused_check_interval_ms == that.paused_check_interval_ms;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Integer.valueOf(this.max_concurrent_sessions), Integer.valueOf(this.max_session_pages), Integer.valueOf(this.max_page_size_mb), Integer.valueOf(this.max_local_query_time_ms), Integer.valueOf(this.client_timeout_sec), Integer.valueOf(this.cancel_timeout_sec), Integer.valueOf(this.paused_check_interval_ms)});
   }

   private Map<String, String> toStringMap() {
      Map<String, String> m = new TreeMap();
      m.put("max_concurrent_sessions", Integer.toString(this.max_concurrent_sessions));
      m.put("max_session_pages", Integer.toString(this.max_session_pages));
      m.put("max_page_size_mb", Integer.toString(this.max_page_size_mb));
      m.put("max_local_query_time_ms", Integer.toString(this.max_local_query_time_ms));
      m.put("client_timeout_sec", Integer.toString(this.client_timeout_sec));
      m.put("cancel_timeout_sec", Integer.toString(this.cancel_timeout_sec));
      m.put("paused_check_interval_ms", Integer.toString(this.paused_check_interval_ms));
      return m;
   }

   public String toString() {
      return '{' + Joiner.on(", ").withKeyValueSeparator("=").join(this.toStringMap()) + '}';
   }
}
