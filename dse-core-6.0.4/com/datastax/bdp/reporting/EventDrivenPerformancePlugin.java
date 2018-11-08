package com.datastax.bdp.reporting;

import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.bean.PluginBean;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

public abstract class EventDrivenPerformancePlugin<T extends CqlWritable> extends AbstractPlugin {
   protected final CqlWriter<T> writer;
   protected final PluginBean bean;
   private final PropertyChangeListener ttlListener = new PropertyChangeListener() {
      public void propertyChange(PropertyChangeEvent evt) {
         EventDrivenPerformancePlugin.this.writer.setTtl(((Integer)evt.getNewValue()).intValue());
      }
   };

   protected EventDrivenPerformancePlugin(CqlWriter<T> writer, PluginBean bean) {
      this.writer = writer;
      this.bean = bean;
   }

   public void onRegister() {
      this.bean.activate(this);
   }

   public void onActivate() {
      this.bean.addPropertyChangeListener("TTL", this.ttlListener);
   }

   public PluginBean getBean() {
      return this.bean;
   }

   public void onPreDeactivate() {
      this.bean.removePropertyChangeListener("TTL", this.ttlListener);
   }
}
