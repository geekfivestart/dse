package org.apache.cassandra.config;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;

public class NodeSyncConfig {
   private static final boolean DEFAULT_ENABLED = true;
   private static final long DEFAULT_RATE_KB = 1024L;
   private static final long DEFAULT_PAGE_SIZE_KB = 512L;
   private static final int DEFAULT_MIN_THREADS = 1;
   private static final int DEFAULT_MAX_THREADS = 8;
   private static final int DEFAULT_MIN_VALIDATIONS = 2;
   private static final int DEFAULT_MAX_VALIDATIONS = 16;
   private static final int DEFAULT_TRACE_TTL_SEC;
   private boolean enabled = true;
   private long rate_in_kb = 1024L;
   private volatile long page_size_in_kb = 512L;
   private volatile int min_threads = 1;
   private volatile int max_threads = 8;
   private volatile int min_inflight_validations = 2;
   private volatile int max_inflight_validations = 16;
   private int trace_ttl_sec;
   public final RateLimiter rateLimiter;

   public NodeSyncConfig() {
      this.trace_ttl_sec = DEFAULT_TRACE_TTL_SEC;
      this.rateLimiter = RateLimiter.create((double)SizeUnit.BYTES.convert(1024L, SizeUnit.KILOBYTES));
   }

   public void setEnabled(boolean enabled) {
      this.enabled = enabled;
   }

   public void setRate_in_kb(long rate_in_kb) {
      this.rate_in_kb = rate_in_kb;
      this.rateLimiter.setRate((double)SizeUnit.BYTES.convert(rate_in_kb, SizeUnit.KILOBYTES));
   }

   public void setPage_size_in_kb(long page_size_in_kb) {
      this.page_size_in_kb = page_size_in_kb;
   }

   public void setMin_threads(int min_threads) {
      this.min_threads = min_threads;
   }

   public void setMax_threads(int max_threads) {
      this.max_threads = max_threads;
   }

   public void setMin_inflight_validations(int min_inflight_validations) {
      this.min_inflight_validations = min_inflight_validations;
   }

   public void setMax_inflight_validations(int max_inflight_validations) {
      this.max_inflight_validations = max_inflight_validations;
   }

   public void setTrace_ttl_sec(int trace_ttl_sec) {
      this.trace_ttl_sec = trace_ttl_sec;
   }

   public boolean isEnabled() {
      return this.enabled;
   }

   public long getPageSize(SizeUnit unit) {
      return unit.convert(this.page_size_in_kb, SizeUnit.KILOBYTES);
   }

   public SizeValue getPageSize() {
      return SizeValue.of(this.page_size_in_kb, SizeUnit.KILOBYTES);
   }

   public void setPageSize(SizeValue pageSize) {
      this.setPage_size_in_kb(pageSize.in(SizeUnit.KILOBYTES));
   }

   public RateValue getRate() {
      return RateValue.of(this.rate_in_kb, RateUnit.KB_S);
   }

   public void setRate(RateValue rate) {
      this.setRate_in_kb(rate.in(RateUnit.KB_S));
   }

   public int getMinThreads() {
      return this.min_threads;
   }

   public int getMaxThreads() {
      return this.max_threads;
   }

   public int getMinInflightValidations() {
      return this.min_inflight_validations;
   }

   public int getMaxInflightValidations() {
      return this.max_inflight_validations;
   }

   public long traceTTL(TimeUnit unit) {
      return unit.convert((long)this.trace_ttl_sec, TimeUnit.SECONDS);
   }

   public void validate() throws ConfigurationException {
      this.validateMinMax((long)this.min_threads, (long)this.max_threads, "threads", false);
      this.validateMinMax((long)this.min_inflight_validations, (long)this.max_inflight_validations, "inflight_segments", false);
      if(this.max_threads > this.max_inflight_validations) {
         throw new ConfigurationException(String.format("max_threads value (%d) should be less than or equal to max_inflight_validations (%d)", new Object[]{Integer.valueOf(this.max_threads), Integer.valueOf(this.max_inflight_validations)}));
      } else if(SizeValue.of(this.page_size_in_kb, SizeUnit.KILOBYTES).in(SizeUnit.BYTES) > 2147483647L) {
         throw new ConfigurationException(String.format("Max page_size_in_kb supported is %d, got: %d", new Object[]{Long.valueOf(SizeValue.of(2147483647L, SizeUnit.BYTES).in(SizeUnit.KILOBYTES)), Long.valueOf(this.page_size_in_kb)}));
      }
   }

   private void validateMinMax(long min, long max, String option, boolean allowZeros) throws ConfigurationException {
      if(min < 0L || !allowZeros && min == 0L) {
         throw new ConfigurationException(String.format("Invalid value for min_%s, must be %spositive: got %d", new Object[]{option, allowZeros?"":"strictly ", Long.valueOf(min)}));
      } else if(max < 0L || !allowZeros && max == 0L) {
         throw new ConfigurationException(String.format("Invalid value for max_%s, must be %spositive: got %d", new Object[]{option, allowZeros?"":"strictly ", Long.valueOf(max)}));
      } else if(min > max) {
         throw new ConfigurationException(String.format("min_%s value (%d) must be less than or equal to max_%s value(%d)", new Object[]{option, Long.valueOf(min), option, Long.valueOf(max)}));
      }
   }

   public boolean equals(Object other) {
      if(!(other instanceof NodeSyncConfig)) {
         return false;
      } else {
         NodeSyncConfig that = (NodeSyncConfig)other;
         return this.enabled == that.enabled && this.rate_in_kb == that.rate_in_kb && this.page_size_in_kb == that.page_size_in_kb && this.min_threads == that.min_threads && this.max_threads == that.max_threads && this.min_inflight_validations == that.min_inflight_validations && this.max_inflight_validations == that.max_inflight_validations && this.trace_ttl_sec == that.trace_ttl_sec;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Boolean.valueOf(this.enabled), Long.valueOf(this.rate_in_kb), Long.valueOf(this.page_size_in_kb), Integer.valueOf(this.min_threads), Integer.valueOf(this.max_threads), Integer.valueOf(this.min_inflight_validations), Integer.valueOf(this.max_inflight_validations), Integer.valueOf(this.trace_ttl_sec)});
   }

   private Map<String, String> toStringMap() {
      Map<String, String> m = new TreeMap();
      m.put("enabled", Boolean.toString(this.enabled));
      m.put("rate_in_kb", Long.toString(this.rate_in_kb));
      m.put("page_size_in_kb", Long.toString(this.page_size_in_kb));
      m.put("min_threads", Integer.toString(this.min_threads));
      m.put("max_threads", Integer.toString(this.max_threads));
      m.put("max_inflight_validations", Integer.toString(this.max_inflight_validations));
      m.put("min_inflight_validations", Integer.toString(this.min_inflight_validations));
      m.put("trace_ttl_sec", Integer.toString(this.trace_ttl_sec));
      return m;
   }

   public String toString() {
      return '{' + Joiner.on(", ").withKeyValueSeparator("=").join(this.toStringMap()) + '}';
   }

   static {
      DEFAULT_TRACE_TTL_SEC = (int)TimeUnit.DAYS.toSeconds(7L);
   }
}
