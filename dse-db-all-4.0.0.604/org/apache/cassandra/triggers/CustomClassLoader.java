package org.apache.cassandra.triggers;

import com.google.common.io.Files;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomClassLoader extends URLClassLoader {
   private static final Logger logger = LoggerFactory.getLogger(CustomClassLoader.class);
   private final Map<String, Class<?>> cache = new ConcurrentHashMap();
   private final ClassLoader parent;

   public CustomClassLoader(ClassLoader parent) {
      super(new URL[0], parent);

      assert parent != null;

      this.parent = this.getParent();
   }

   public CustomClassLoader(ClassLoader parent, File classPathDir) {
      super(new URL[0], parent);

      assert parent != null;

      this.parent = this.getParent();
      this.addClassPath(classPathDir);
   }

   public void addClassPath(File dir) {
      if(dir != null && dir.exists()) {
         FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
               return name.endsWith(".jar");
            }
         };
         File[] var3 = dir.listFiles(filter);
         int var4 = var3.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            File inputJar = var3[var5];
            File lib = new File(System.getProperty("java.io.tmpdir"), "lib");
            if(!lib.exists()) {
               lib.mkdir();
               lib.deleteOnExit();
            }

            try {
               File out = File.createTempFile("cassandra-", ".jar", lib);
               out.deleteOnExit();
               logger.info("Loading new jar {}", inputJar.getAbsolutePath());
               Files.copy(inputJar, out);
               this.addURL(out.toURI().toURL());
            } catch (IOException var9) {
               throw new IOError(var9);
            }
         }

      }
   }

   public Class<?> loadClass(String name) throws ClassNotFoundException {
      Class<?> clazz = (Class)this.cache.get(name);
      return clazz == null?this.loadClassInternal(name):clazz;
   }

   public synchronized Class<?> loadClassInternal(String name) throws ClassNotFoundException {
      try {
         return this.parent.loadClass(name);
      } catch (ClassNotFoundException var3) {
         logger.trace("Class not found using parent class loader,", var3);
         Class<?> clazz = this.findClass(name);
         this.cache.put(name, clazz);
         return clazz;
      }
   }
}
