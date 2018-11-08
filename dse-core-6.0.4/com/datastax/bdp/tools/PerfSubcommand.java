package com.datastax.bdp.tools;

import com.datastax.bdp.plugin.bean.AsyncSnapshotPluginMXBean;
import com.datastax.bdp.plugin.bean.PluginBean;
import com.datastax.bdp.plugin.bean.PluginMXBean;
import java.beans.PropertyVetoException;
import javax.management.MalformedObjectNameException;

public abstract class PerfSubcommand implements Command {
   private final String command;
   private final Class<? extends PluginBean> beanClass;
   private final Class<? extends PluginMXBean> mxClass;

   protected PerfSubcommand(String command, Class<? extends PluginBean> beanClass) {
      this(command, beanClass, AsyncSnapshotPluginMXBean.class);
   }

   protected PerfSubcommand(String command, Class<? extends PluginBean> beanClass, Class<? extends PluginMXBean> mxClass) {
      this.command = command;
      this.beanClass = beanClass;
      this.mxClass = mxClass;
   }

   public String getOptionsHelp() {
      return null;
   }

   public boolean isEnabled(String[] arguments) {
      if(arguments.length == 2) {
         String var2 = arguments[1];
         byte var3 = -1;
         switch(var2.hashCode()) {
         case -1298848381:
            if(var2.equals("enable")) {
               var3 = 0;
            }
            break;
         case 1671308008:
            if(var2.equals("disable")) {
               var3 = 1;
            }
         }

         switch(var3) {
         case 0:
            return true;
         case 1:
            return false;
         }
      }

      throw new IllegalArgumentException("Expecting 'enable' or 'disable'");
   }

   public void setEnabled(NodeJmxProxyPool dnp, boolean enabled) throws PropertyVetoException {
      this.getProxy(dnp).setEnabled(enabled);
   }

   public String getName() {
      return this.command;
   }

   public void execute(NodeJmxProxyPool dnp, String[] arguments) throws PropertyVetoException {
      this.setEnabled(dnp, this.isEnabled(arguments));
   }

   protected PluginMXBean getProxy(NodeJmxProxyPool dnp) {
      try {
         return (PluginMXBean)dnp.makePerfProxy(this.beanClass, this.mxClass);
      } catch (MalformedObjectNameException var3) {
         throw new RuntimeException(var3);
      }
   }

   public abstract String getHelp();
}
