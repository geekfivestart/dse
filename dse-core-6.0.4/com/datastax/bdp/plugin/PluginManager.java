package com.datastax.bdp.plugin;

import com.datastax.bdp.server.LifecycleAware;
import com.datastax.bdp.util.DseUtil;
import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PluginManager implements LifecycleAware {
   private static final Logger logger = LoggerFactory.getLogger(PluginManager.class);
   private static final Duration PLUGIN_ACTIVATION_RETRY_INTERVAL = Duration.standardSeconds(1L);
   private final ConcurrentMap<Class<? extends IPlugin>, IPlugin> activePlugins;
   private final ConcurrentMap<Class<? extends IPlugin>, IPlugin> inactivePlugins;
   private final Multimap<Class<? extends IPlugin>, Class<? extends IPlugin>> dependants;
   private final LinkedHashSet<IPlugin> pendingPostStop = new LinkedHashSet();
   private volatile boolean isShutdown = false;
   private volatile boolean isShuttingDown = false;
   private volatile boolean isNativeTransportActive = false;
   private final Injector injector;

   @Inject
   public PluginManager(@Nullable Injector injector) {
      this.injector = injector;
      this.activePlugins = new ConcurrentHashMap();
      this.inactivePlugins = new ConcurrentHashMap();
      this.dependants = Multimaps.newSetMultimap(new HashMap(), HashSet::<init>);
   }

   public synchronized void postSetup() {
      if(this.injector != null) {
         logger.debug("Registering plugins");
         Iterator var1 = this.injector.getAllBindings().keySet().iterator();

         while(var1.hasNext()) {
            Key<?> key = (Key)var1.next();
            Class<?> pluginClass = key.getTypeLiteral().getRawType();
            if(IPlugin.class.isAssignableFrom(pluginClass) && pluginClass.isAnnotationPresent(DsePlugin.class)) {
               this.register(pluginClass, (p) -> {
               });
            }
         }

         logger.debug("Plugins registration finished");
      }

   }

   public synchronized void preStart() {
      this.activate();
   }

   public synchronized void postStart() {
      if(!this.isNativeTransportActive) {
         List<String> notStartedPlugins = (List)this.getInactivePlugins().stream().filter((p) -> {
            return p.isEnabled() && p.requiresNativeTransport();
         }).map((p) -> {
            return p.getClass().getName();
         }).collect(Collectors.toList());
         if(!notStartedPlugins.isEmpty()) {
            logger.warn("Some plugins could not be started because native CQL transport is disabled: " + notStartedPlugins);
         }
      }

   }

   public synchronized void postStartNativeTransport() {
      if(!this.isNativeTransportActive) {
         this.isNativeTransportActive = true;
         this.activate();
      }

   }

   public synchronized void preStopNativeTransport() {
      if(this.isNativeTransportActive) {
         this.deactivate();
         this.isNativeTransportActive = false;
      }

   }

   public synchronized void preStop() {
      this.isShuttingDown = true;
      this.deactivate();
   }

   public synchronized void postStop() {
      this.pendingPostStop.stream().forEachOrdered((plugin) -> {
         try {
            plugin.onPostDeactivate();
         } catch (Exception var2) {
            interruptIfNeeded(var2);
            logger.error(String.format("Failed to post-deactivate plugin: %s - %s", new Object[]{plugin, var2}), var2);
         }

      });
      this.pendingPostStop.clear();
      this.isShutdown = true;
      this.isShuttingDown = false;
      logger.info("Plugins are stopped.");
   }

   public Collection<IPlugin> getActivePlugins() {
      return Collections.unmodifiableCollection(this.activePlugins.values());
   }

   public Collection<IPlugin> getInactivePlugins() {
      return Collections.unmodifiableCollection(this.inactivePlugins.values());
   }

   public synchronized <T extends IPlugin> T getActivePlugin(Class<T> pluginClass) {
      return (IPlugin)this.activePlugins.get(pluginClass);
   }

   private synchronized boolean isPluginActive(IPlugin plugin) {
      if(this.activePlugins.containsKey(plugin.getClass())) {
         return true;
      } else if(this.inactivePlugins.containsKey(plugin.getClass())) {
         return false;
      } else {
         throw new PluginManager.PluginNotRegisteredException(plugin.getClass());
      }
   }

   private void register(Class<? extends IPlugin> pluginClass, Consumer<Class<? extends IPlugin>> dependencyVisitor) {
      if(this.injector != null) {
         Iterator var3 = PluginUtil.getPluginDependencies(pluginClass).iterator();

         while(var3.hasNext()) {
            Class<? extends IPlugin> dep = (Class)var3.next();
            this.register(dep, (p) -> {
               dependencyVisitor.accept(p);
               this.dependants.put(p, pluginClass);
            });
         }

         try {
            if(!this.inactivePlugins.containsKey(pluginClass) && !this.activePlugins.containsKey(pluginClass)) {
               IPlugin plugin = (IPlugin)this.injector.getInstance(pluginClass);
               plugin.setPluginManager(this);
               this.registerDirect(plugin);
            }

            dependencyVisitor.accept(pluginClass);
         } catch (PluginManager.DuplicatePluginException var5) {
            assert false;
         }
      }

   }

   private void registerDirect(IPlugin plugin) throws PluginManager.DuplicatePluginException {
      IPlugin currentPlugin = (IPlugin)this.activePlugins.get(plugin.getClass());
      if(currentPlugin == null) {
         currentPlugin = (IPlugin)this.inactivePlugins.putIfAbsent(plugin.getClass(), plugin);
      }

      if(currentPlugin != null) {
         throw new PluginManager.DuplicatePluginException(currentPlugin);
      } else {
         try {
            plugin.onRegister();
            logger.debug("Registered plugin: {}", plugin);
         } catch (Exception var4) {
            logger.error(String.format("Failed to register plugin %s", new Object[]{plugin}), var4);
            throw new PluginManager.PluginRuntimeException("Registration failed", plugin, var4);
         }
      }
   }

   private void activate() {
      logger.info("Activating plugins which {} native transport", this.isNativeTransportActive?"require":"do not require");

      try {
         Iterator var1 = (new ArrayList(this.inactivePlugins.keySet())).iterator();

         while(var1.hasNext()) {
            Class<? extends IPlugin> pluginClass = (Class)var1.next();
            this.activate((IPlugin)this.inactivePlugins.get(pluginClass), false);
         }

         logger.info("Activation of plugins which {} native transport finished", this.isNativeTransportActive?"require":"do not require");
      } catch (RuntimeException var4) {
         logger.info("All plugins will be deactivated because activation of some plugins failed");

         try {
            this.deactivate();
         } catch (RuntimeException var3) {
            ;
         }

         throw var4;
      }
   }

   public synchronized void activate(IPlugin plugin, boolean force) {
      if(force && plugin != null) {
         logger.info("Requested to force activate plugin: {}", plugin);
      }

      if(this.shouldActivatePlugin(plugin, force)) {
         Iterator var3 = PluginUtil.getPluginDependencies(plugin.getClass()).iterator();

         while(var3.hasNext()) {
            Class<? extends IPlugin> dependencyClass = (Class)var3.next();
            this.activate((IPlugin)this.inactivePlugins.get(dependencyClass), force);
         }

         this.activateDirect(plugin);
      }

   }

   private boolean shouldActivatePlugin(IPlugin plugin, boolean force) {
      if(plugin != null && !this.isPluginActive(plugin)) {
         if(!this.isPluginEnabled(plugin) && !force) {
            logger.debug("Denying activation of plugin {} because it is disabled", plugin);
            return false;
         } else if(this.isNativeTransportActive) {
            return true;
         } else if(plugin.requiresNativeTransport()) {
            logger.debug("Denying activation of plugin {} because it requires native transport", plugin);
            return false;
         } else {
            boolean result = PluginUtil.getPluginDependencies(plugin.getClass()).stream().noneMatch((depClass) -> {
               return ((Boolean)this.getInstance(depClass).map(IPlugin::requiresNativeTransport).orElse(Boolean.valueOf(false))).booleanValue();
            });
            if(!result) {
               logger.debug("Denying activation of plugin {} because some of its dependencies require native transport", plugin);
            }

            return result;
         }
      } else {
         return false;
      }
   }

   private boolean isPluginEnabled(IPlugin plugin) {
      return plugin == null?false:(plugin.isEnabled()?true:this.getDependants(plugin.getClass()).stream().anyMatch((depClass) -> {
         return ((Boolean)this.getInstance(depClass).map(this::isPluginEnabled).orElse(Boolean.valueOf(false))).booleanValue();
      }));
   }

   private void activateDirect(IPlugin plugin) {
      while(!this.isPluginActive(plugin) && !this.isShutdown && !this.isShuttingDown && !Thread.currentThread().isInterrupted()) {
         try {
            logger.info("Activating plugin: {}", plugin);
            this.checkHasNoInactiveDependencies(plugin.getClass());
            plugin.setupSchema();
            plugin.onActivate();
            this.makeActive(plugin);
         } catch (Exception var5) {
            if(!DseUtil.isCausedByUnavailability(var5)) {
               throw new PluginManager.PluginRuntimeException("Activation failed", plugin, var5);
            }

            logger.info("No enough available nodes to start plugin {}. Trying once again...", plugin);

            try {
               Thread.sleep(PLUGIN_ACTIVATION_RETRY_INTERVAL.getMillis());
            } catch (InterruptedException var4) {
               logger.warn("Retry loop has been interrupted for plugin {}. Giving up.", plugin);
               Thread.currentThread().interrupt();
               throw new PluginManager.PluginRuntimeException("Activation failed", plugin, var5);
            }
         }
      }

   }

   private void checkHasNoInactiveDependencies(Class<? extends IPlugin> pluginClass) {
      boolean allDependenciesActive = PluginUtil.getPluginDependencies(pluginClass).stream().map(this::getInstance).allMatch((plugin) -> {
         return ((Boolean)plugin.map(IPlugin::isActive).orElse(Boolean.valueOf(false))).booleanValue();
      });

      assert allDependenciesActive : String.format("Some dependencies of plugin %s are inactive.", new Object[]{pluginClass.getName()});

   }

   private void makeActive(IPlugin plugin) {
      this.activePlugins.putIfAbsent(plugin.getClass(), plugin);
      plugin.setActive(true);
      this.inactivePlugins.remove(plugin.getClass(), plugin);
      logger.debug("Plugin activated: {}", plugin);
   }

   private synchronized void deactivate() {
      logger.info("Deactivating plugins which {} native transport", this.isNativeTransportActive?"require":"do not require");
      boolean success = true;

      Class pluginClass;
      for(Iterator var2 = (new ArrayList(this.activePlugins.keySet())).iterator(); var2.hasNext(); success &= this.deactivate((IPlugin)this.activePlugins.get(pluginClass), false)) {
         pluginClass = (Class)var2.next();
      }

      if(!success) {
         throw new RuntimeException("Failed to deactivate some plugins. See messages in log");
      } else {
         logger.info("Deactivation of plugins which {} native transport finished", this.isNativeTransportActive?"require":"do not require");
      }
   }

   public synchronized boolean deactivate(IPlugin plugin, boolean force) {
      if(!this.shouldDeactivatePlugin(plugin, force)) {
         return true;
      } else {
         boolean noFailuresInDeps = this.getDependants(plugin.getClass()).stream().map((depClass) -> {
            return (IPlugin)this.getInstance(depClass).orElse((Object)null);
         }).allMatch((p) -> {
            return this.deactivate(p, force);
         });
         this.checkNoActiveDependants(plugin.getClass());
         boolean noFailures = this.deactivateDirect(plugin);
         return noFailuresInDeps && noFailures;
      }
   }

   private boolean shouldDeactivatePlugin(IPlugin plugin, boolean force) {
      return plugin != null && this.isPluginActive(plugin)?(force?true:(!this.isNativeTransportActive?true:(plugin.requiresNativeTransport()?true:PluginUtil.getPluginDependencies(plugin.getClass()).stream().anyMatch((depClass) -> {
         return ((Boolean)this.getInstance(depClass).map(IPlugin::requiresNativeTransport).orElse(Boolean.valueOf(false))).booleanValue();
      })))):false;
   }

   private boolean deactivateDirect(IPlugin plugin) {
      logger.info("Deactivating plugin: {}", plugin);
      boolean needsPostStopAction = plugin.needsPostDeactivate();

      boolean var4;
      try {
         plugin.onPreDeactivate();
         logger.info("Plugin deactivated: {}", plugin);
         boolean var3 = true;
         return var3;
      } catch (Exception var8) {
         interruptIfNeeded(var8);
         logger.error(String.format("Failed to pre-deactivate plugin: %s - %s", new Object[]{plugin, var8}), var8);
         var4 = false;
      } finally {
         if(this.activePlugins.remove(plugin.getClass()) != null) {
            this.inactivePlugins.put(plugin.getClass(), plugin);
         }

         plugin.setActive(false);
         if(needsPostStopAction) {
            this.pendingPostStop.add(plugin);
         }

      }

      return var4;
   }

   private void checkNoActiveDependants(Class<? extends IPlugin> pluginClass) {
      boolean hasActiveDependants = this.getDependants(pluginClass).stream().map(this::getInstance).anyMatch((plugin) -> {
         return ((Boolean)plugin.map(this::isPluginActive).orElse(Boolean.valueOf(false))).booleanValue();
      });

      assert !hasActiveDependants : String.format("Some plugins that depends on plugin %s are still active.", new Object[]{pluginClass.getName()});

   }

   private Collection<Class<? extends IPlugin>> getDependants(Class<? extends IPlugin> pluginClass) {
      return (Collection)Optional.ofNullable(this.dependants.get(pluginClass)).orElse(Collections.emptySet());
   }

   private Optional<IPlugin> getInstance(Class<? extends IPlugin> pluginClass) {
      return Optional.ofNullable(this.inactivePlugins.getOrDefault(pluginClass, this.activePlugins.get(pluginClass)));
   }

   private static void interruptIfNeeded(Throwable ex) {
      if(ex instanceof InterruptedException) {
         Thread.currentThread().interrupt();
      }

   }

   public static class PluginNotRegisteredException extends IllegalArgumentException {
      public PluginNotRegisteredException(Class<? extends IPlugin> pluginClass) {
         super(String.format("Plugin %s is not registered.", new Object[]{pluginClass.getName()}));
      }
   }

   public static class PluginRuntimeException extends RuntimeException {
      private static final long serialVersionUID = 1L;
      public final IPlugin currentPlugin;

      public PluginRuntimeException(String message, IPlugin currentPlugin, Throwable cause) {
         super(String.format("Plugin %s failed: %s - %s", new Object[]{currentPlugin, message, cause}), cause);
         PluginManager.interruptIfNeeded(cause);
         this.currentPlugin = currentPlugin;
      }
   }

   public static class DuplicatePluginException extends Exception {
      private static final long serialVersionUID = 1L;
      public final IPlugin currentPlugin;

      public DuplicatePluginException(IPlugin currentPlugin) {
         this.currentPlugin = currentPlugin;
      }

      public String toString() {
         return "Plugin already initialized: " + this.currentPlugin;
      }
   }
}
