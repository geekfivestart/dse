package com.datastax.bdp.plugin.bean;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyVetoException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AsyncSnapshotInfoBean extends SnapshotInfoBean implements AsyncSnapshotPluginMXBean {
   private static final Logger logger = LoggerFactory.getLogger(AsyncSnapshotInfoBean.class);
   public static final String ASYNC_WRITERS_PROPERTY_NAME = "asyncWriters";
   private final AtomicInteger asyncWriters = new AtomicInteger();

   public AsyncSnapshotInfoBean() {
   }

   public int getAsyncWriters() {
      return this.asyncWriters.get();
   }

   public synchronized void setAsyncWriters(int asyncWriters) throws PropertyVetoException {
      int oldValue = this.asyncWriters.get();
      if(oldValue != asyncWriters) {
         if(asyncWriters < 0) {
            throw new PropertyVetoException("Must be >= 0", new PropertyChangeEvent(this, "asyncWriters", Integer.valueOf(this.asyncWriters.get()), Integer.valueOf(asyncWriters)));
         }

         this.fireVetoableChange("asyncWriters", Integer.valueOf(oldValue), Integer.valueOf(asyncWriters));
         this.asyncWriters.set(asyncWriters);
         this.firePropertyChange("asyncWriters", Integer.valueOf(oldValue), Integer.valueOf(asyncWriters));
      }

   }
}
