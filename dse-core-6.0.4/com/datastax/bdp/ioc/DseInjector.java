package com.datastax.bdp.ioc;

import com.datastax.bdp.DseModule;
import com.datastax.bdp.server.AbstractDseModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.lang.reflect.InvocationTargetException;

public class DseInjector {
   public static final String MODULE_CLASS_PROP_NAME = "dse.module.class";
   private static volatile Injector instance;
   private static volatile AbstractModule module;

   public DseInjector() {
   }

   public static Injector get() {
      if(instance == null) {
         Class var0 = DseInjector.class;
         synchronized(DseInjector.class) {
            if(instance == null) {
               instance = Guice.createInjector(new Module[]{getModule()});
            }
         }
      }

      return instance;
   }

   private static AbstractModule getModule() {
      try {
         if(module == null) {
            String className = System.getProperty("dse.module.class");
            module = (AbstractModule)(className == null?new DseModule():(AbstractDseModule)Thread.currentThread().getContextClassLoader().loadClass(className).getConstructor(new Class[0]).newInstance(new Object[0]));
         }

         return module;
      } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | ClassNotFoundException | InstantiationException var1) {
         throw new RuntimeException("Error getting module", var1);
      }
   }

   public static synchronized void set(Injector injector) {
      instance = injector;
   }

   public static void setModule(AbstractModule _module) {
      module = _module;
   }
}
