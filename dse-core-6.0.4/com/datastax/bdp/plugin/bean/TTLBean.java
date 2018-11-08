package com.datastax.bdp.plugin.bean;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyVetoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TTLBean extends PluginBean implements TTLMXBean {
   Logger logger = LoggerFactory.getLogger(TTLBean.class);
   public static final String TTL_PROPERTY_NAME = "TTL";

   public TTLBean(boolean enabled, int ttl) {
      try {
         this.setTTL(ttl);
         this.maybeEnable(enabled);
      } catch (PropertyVetoException var4) {
         throw new AssertionError(var4);
      }
   }

   public int getTTL() {
      return this.ttl.get();
   }

   public synchronized void setTTL(int ttl) throws PropertyVetoException {
      int oldValue = this.ttl.get();
      if(oldValue != ttl) {
         if(ttl < 0) {
            throw new PropertyVetoException("Must be >= 0", new PropertyChangeEvent(this, "TTL", Integer.valueOf(this.ttl.get()), Integer.valueOf(ttl)));
         }

         this.fireVetoableChange("TTL", Integer.valueOf(oldValue), Integer.valueOf(ttl));
         this.ttl.set(ttl);
         this.firePropertyChange("TTL", Integer.valueOf(oldValue), Integer.valueOf(ttl));
         if(this.isEnabled()) {
            this.logger.info("Setting TTL to {}", Integer.valueOf(ttl));
         }
      }

   }
}
