package com.datastax.bdp.server;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDseModule extends AbstractModule {
   private static final Logger logger = LoggerFactory.getLogger(AbstractDseModule.class);

   public AbstractDseModule() {
   }

   protected void configure() {
      this.binder().requireExplicitBindings();
      AbstractModule[] var1 = this.getRequiredModules();
      int var2 = var1.length;

      int var3;
      for(var3 = 0; var3 < var2; ++var3) {
         AbstractModule requiredModule = var1[var3];
         this.install(requiredModule);
      }

      String[] var10 = this.getOptionalModuleNames();
      var2 = var10.length;

      for(var3 = 0; var3 < var2; ++var3) {
         String name = var10[var3];

         try {
            Class<?> aClass = Class.forName(name);
            this.install((Module)aClass.getConstructor(new Class[0]).newInstance(new Object[0]));
         } catch (ClassNotFoundException var7) {
            logger.warn("Module {} not found. Some functionality may be missing", name);
         } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassCastException var8) {
            logger.error("Can't load module " + name, var8);
         } catch (InvocationTargetException var9) {
            Throwable reported = var9.getTargetException();
            if(reported instanceof ExceptionInInitializerError) {
               reported = var9.getCause();
            }

            throw new RuntimeException("Unable to initialize module " + name, reported);
         }
      }

   }

   public abstract String[] getOptionalModuleNames();

   public abstract AbstractModule[] getRequiredModules();
}
