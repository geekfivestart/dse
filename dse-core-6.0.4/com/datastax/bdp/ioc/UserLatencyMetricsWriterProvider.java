package com.datastax.bdp.ioc;

import com.datastax.bdp.cassandra.metrics.UserLatencyMetricsWriter;
import com.datastax.bdp.cassandra.metrics.UserMetrics;
import com.datastax.bdp.plugin.bean.UserLatencyTrackingBean;
import com.datastax.bdp.system.TimeSource;
import com.google.inject.Provider;
import javax.inject.Inject;

public class UserLatencyMetricsWriterProvider implements Provider<UserLatencyMetricsWriter> {
   private final UserLatencyTrackingBean userLatencyTrackingBean;
   private final TimeSource timeSource;

   @Inject
   public UserLatencyMetricsWriterProvider(UserLatencyTrackingBean userLatencyTrackingBean, TimeSource timeSource) {
      this.userLatencyTrackingBean = userLatencyTrackingBean;
      this.timeSource = timeSource;
   }

   public UserLatencyMetricsWriter get() {
      return new UserLatencyMetricsWriter(this.userLatencyTrackingBean, new UserMetrics(this.userLatencyTrackingBean, this.timeSource));
   }
}
