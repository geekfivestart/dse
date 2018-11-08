package com.datastax.bdp.plugin;

public interface IPlugin {
   boolean isActive();

   void setActive(boolean var1);

   void setPluginManager(PluginManager var1);

   PluginManager getPluginManager();

   void setupSchema() throws Exception;

   void onRegister();

   void onActivate();

   void onPreDeactivate();

   void onPostDeactivate();

   boolean needsPostDeactivate();

   boolean isEnabled();

   boolean requiresNativeTransport();

   public static class PluginNotActiveException extends Exception {
      public PluginNotActiveException(String message) {
         super(message);
      }
   }
}
