package com.datastax.bdp.plugin;

public abstract class AbstractPlugin implements IPlugin {
   private boolean active;
   private PluginManager pluginManager;

   public AbstractPlugin() {
   }

   public boolean isActive() {
      return this.active;
   }

   public void setActive(boolean active) {
      this.active = active;
   }

   public final void setPluginManager(PluginManager pluginManager) {
      this.pluginManager = pluginManager;
   }

   public final PluginManager getPluginManager() {
      return this.pluginManager;
   }

   public void setupSchema() throws Exception {
   }

   public void onRegister() {
   }

   public void onActivate() {
   }

   public void onPreDeactivate() {
   }

   public void onPostDeactivate() {
   }

   public boolean needsPostDeactivate() {
      return false;
   }

   public boolean isEnabled() {
      return false;
   }

   public boolean requiresNativeTransport() {
      return false;
   }

   public String toString() {
      return this.getClass().getName();
   }
}
