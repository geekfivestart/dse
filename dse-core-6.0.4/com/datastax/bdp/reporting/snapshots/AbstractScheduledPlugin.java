package com.datastax.bdp.reporting.snapshots;

import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.datastax.bdp.util.Addresses;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.net.InetAddress;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractScheduledPlugin<T extends SnapshotInfoBean> extends AbstractPlugin {
   private static final Logger logger = LoggerFactory.getLogger(AbstractScheduledPlugin.class);
   private final ThreadPoolPlugin threadPool;
   private final boolean userActivated;
   private final T mbean;
   protected InetAddress nodeAddress;
   private volatile ScheduledFuture<?> scheduledTask;
   private final PropertyChangeListener refreshRateListener = new PropertyChangeListener() {
      public void propertyChange(PropertyChangeEvent evt) {
         AbstractScheduledPlugin.this.scheduledTask.cancel(false);
         AbstractScheduledPlugin.this.scheduleTask();
      }
   };

   protected AbstractScheduledPlugin(ThreadPoolPlugin threadPool, T mbean, boolean userActivated) {
      this.threadPool = threadPool;
      this.mbean = mbean;
      this.userActivated = userActivated;
      this.nodeAddress = Addresses.Internode.getBroadcastAddress();
   }

   protected abstract Runnable getTask();

   public ThreadPoolPlugin getThreadPool() {
      return this.threadPool;
   }

   public final void onRegister() {
      super.onRegister();
      if(this.userActivated) {
         this.mbean.activate(this);
      }

   }

   public boolean isEnabled() {
      return this.mbean.isEnabled();
   }

   protected int getRefreshPeriod() {
      return this.mbean.getRefreshRate();
   }

   protected int getInitialDelay() {
      return 0;
   }

   protected int getTTL() {
      return this.getRefreshPeriod() * 2 / 1000;
   }

   private void scheduleTask() {
      this.scheduledTask = this.threadPool.scheduleAtFixedRate(this.getLoggedTask(), (long)this.getInitialDelay(), (long)this.getRefreshPeriod(), TimeUnit.MILLISECONDS);
   }

   public void onActivate() {
      this.scheduleTask();
      this.mbean.addPropertyChangeListener("refreshRate", this.refreshRateListener);
   }

   public void onPreDeactivate() {
      this.mbean.removePropertyChangeListener("refreshRate", this.refreshRateListener);
      this.scheduledTask.cancel(false);
   }

   protected final T getMbean() {
      return this.mbean;
   }

   private Runnable getLoggedTask() {
      Runnable task = this.getTask();
      String beanName = PerformanceObjectsController.getPerfBeanName(this.mbean.getClass());
      return () -> {
         try {
            task.run();
         } catch (Throwable var3) {
            logger.error(String.format("A scheduled task for %s failed. No further scheduled tasks for this plugin will be attempted", new Object[]{beanName}), var3);
            throw var3;
         }
      };
   }
}
