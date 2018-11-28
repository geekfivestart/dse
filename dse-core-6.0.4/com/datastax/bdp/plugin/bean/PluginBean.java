package com.datastax.bdp.plugin.bean;

import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.plugin.IPlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.util.MapBuilder;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.beans.PropertyVetoException;
import java.beans.VetoableChangeListener;
import java.beans.VetoableChangeSupport;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PluginBean implements PluginMXBean {
   private static final Logger logger = LoggerFactory.getLogger(PluginBean.class);
   public static final String ENABLED_PROPERTY_NAME = "enabled";
   protected final AtomicInteger ttl = new AtomicInteger();
   private final PropertyChangeSupport pcs = new PropertyChangeSupport(this);
   private final VetoableChangeSupport vcs = new VetoableChangeSupport(this);
   private final AtomicBoolean enabled = new AtomicBoolean(false);

   public PluginBean() {
   }

   public boolean isCompatibleWithWorkload() {
      return true;
   }

   public boolean isEnabled() {
      return this.enabled.get();
   }

   public synchronized void setEnabled(boolean newValue) throws PropertyVetoException {
      boolean oldValue = this.enabled.get();
      if(oldValue != newValue) {
         this.fireVetoableChange("enabled", Boolean.valueOf(oldValue), Boolean.valueOf(newValue));
         this.enabled.set(newValue);
         this.firePropertyChange("enabled", Boolean.valueOf(oldValue), Boolean.valueOf(newValue));
         logger.info("{} plugin is now {}", PerformanceObjectsController.getPerfBeanName(this.getClass()), newValue?"enabled":"disabled");
      }

   }

   public void maybeEnable(boolean newValue) {
      if(newValue && !this.isCompatibleWithWorkload()) {
         logger.info("Cannot enable {} as it is incompatible with the current workload", PerformanceObjectsController.getPerfBeanName(this.getClass()));
      } else {
         try {
            this.setEnabled(newValue);
         } catch (PropertyVetoException var3) {
            throw new IllegalStateException(var3);
         }
      }

   }

   public void activate(IPlugin plugin) {
      logger.debug("Plugin {} listening on enabled property of {}", plugin.getClass().getName(), this.getClass().getSimpleName());
      this.addVetoableChangeListener("enabled", (evt) -> {
         boolean activating = ((Boolean)evt.getNewValue()).booleanValue();

         try {
            if(activating) {
               if(!this.isCompatibleWithWorkload()) {
                  throw new PropertyVetoException(plugin.getClass().getSimpleName() + "is not compatible with the current workload", evt);
               }

               logger.debug("Activating {}", plugin.getClass().getSimpleName());
               plugin.getPluginManager().activate(plugin, true);
            } else {
               logger.debug("Deactivating {}", plugin.getClass().getSimpleName());
               plugin.getPluginManager().deactivate(plugin, true);
            }

         } catch (RuntimeException var5) {
            logger.warn(MessageFormat.format("Error {0} plugin {1}", new Object[]{activating?"activating":"deactivating", plugin.getClass().getSimpleName()}), var5);
            throw new PropertyVetoException(var5.getMessage(), evt);
         }
      });
   }

   public void addPropertyChangeListener(String propertyName, PropertyChangeListener listener) {
      this.pcs.addPropertyChangeListener(propertyName, listener);
   }

   public void removePropertyChangeListener(String propertyName, PropertyChangeListener listener) {
      this.pcs.removePropertyChangeListener(propertyName, listener);
   }

   public void addVetoableChangeListener(String propertyName, VetoableChangeListener listener) {
      this.vcs.addVetoableChangeListener(propertyName, listener);
   }

   public void removeVetoableChangeListener(String propertyName, VetoableChangeListener listener) {
      this.vcs.removeVetoableChangeListener(propertyName, listener);
   }

   protected void firePropertyChange(String propertyName, Object oldValue, Object newValue) {
      this.pcs.firePropertyChange(propertyName, oldValue, newValue);
   }

   protected void fireVetoableChange(String propertyName, Object oldValue, Object newValue) throws PropertyVetoException {
      this.vcs.fireVetoableChange(propertyName, oldValue, newValue);
   }

   public void registerMBean() {
      String beanName = PerformanceObjectsController.getPerfBeanName(this.getClass());
      logger.debug("Registering JMX interface for {}", beanName);
      JMX.registerMBean(this, JMX.Type.PERF_OBJECTS, MapBuilder.<String,String>immutable().withKeys(new String[]{"name"}).withValues(new String[]{beanName}).build());
   }
}
