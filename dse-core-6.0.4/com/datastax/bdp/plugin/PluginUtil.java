package com.datastax.bdp.plugin;

import com.google.common.base.Preconditions;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class PluginUtil {
   public PluginUtil() {
   }

   public static void checkPluginClass(Class<? extends IPlugin> pluginClass) {
      Preconditions.checkArgument(!Modifier.isAbstract(pluginClass.getModifiers()) && !Modifier.isInterface(pluginClass.getModifiers()), String.format("Plugin class %s is abstract or interface", new Object[]{pluginClass.getName()}));
      DsePlugin annotation = (DsePlugin)pluginClass.getAnnotation(DsePlugin.class);
      Preconditions.checkNotNull(annotation, String.format("Plugin class %s has to be annotated with @DsePlugin", new Object[]{pluginClass.getName()}));
   }

   public static Set<Class<? extends IPlugin>> getPluginDependencies(Class<? extends IPlugin> pluginClass) {
      Set<Class<? extends IPlugin>> deps = new LinkedHashSet();
      addDeps(new HashSet(), deps, pluginClass);
      deps.remove(pluginClass);
      return deps;
   }

   private static void addDeps(Set<Class<? extends IPlugin>> traversedPlugins, Set<Class<? extends IPlugin>> depsSet, Class<? extends IPlugin> pluginClass) {
      checkPluginClass(pluginClass);
      Class<? extends IPlugin>[] deps = getDirectPluginDependencies(pluginClass);
      Class[] var4 = deps;
      int var5 = deps.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         Class<? extends IPlugin> dep = var4[var6];
         if(traversedPlugins.contains(dep)) {
            throw new IllegalArgumentException("Discovered circular dependency of plugin: " + pluginClass.getName());
         }

         traversedPlugins.add(dep);
         addDeps(traversedPlugins, depsSet, dep);
         traversedPlugins.remove(dep);
      }

      depsSet.add(pluginClass);
   }

   public static Class<? extends IPlugin>[] getDirectPluginDependencies(Class<? extends IPlugin> pluginClass) {
      DsePlugin annotation = (DsePlugin)pluginClass.getAnnotation(DsePlugin.class);
      return annotation.dependsOn();
   }
}
