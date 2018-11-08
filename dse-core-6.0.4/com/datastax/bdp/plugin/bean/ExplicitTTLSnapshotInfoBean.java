package com.datastax.bdp.plugin.bean;

import com.google.common.collect.ImmutableMap;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyVetoException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public abstract class ExplicitTTLSnapshotInfoBean extends SnapshotInfoBean implements ExplicitTTLSnapshotInfoMXBean {
   protected final AtomicInteger ttl = new AtomicInteger();

   public ExplicitTTLSnapshotInfoBean() {
   }

   public int getTTL() {
      return this.ttl.get();
   }

   public synchronized void setTTL(int ttl) throws PropertyVetoException {
      int oldValue = this.ttl.get();
      if(oldValue != ttl) {
         if(ttl < 0) {
            PropertyChangeEvent event = new PropertyChangeEvent(this, "TTL", Integer.valueOf(this.ttl.get()), Integer.valueOf(ttl));
            throw new PropertyVetoException("Must be >= 0", event);
         }

         this.fireVetoableChange("TTL", Integer.valueOf(oldValue), Integer.valueOf(ttl));
         this.ttl.set(ttl);
         this.firePropertyChange("TTL", Integer.valueOf(oldValue), Integer.valueOf(ttl));
         if(this.isEnabled()) {
            LoggerFactory.getLogger(this.getClass()).info("Setting TTL to {}", Integer.valueOf(ttl));
         }
      }

   }

   public String getConfigSetting() {
      return (new Yaml()).dump(ImmutableMap.builder().put(this.getConfigName(), ImmutableMap.builder().put("enabled", "" + this.isEnabled()).put("ttl_seconds", "" + this.ttl.toString()).put("refresh_rate_ms", "" + this.getRefreshRate()).build()).build());
   }
}
