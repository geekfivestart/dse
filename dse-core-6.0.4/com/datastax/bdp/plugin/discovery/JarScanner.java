package com.datastax.bdp.plugin.discovery;

import com.google.common.collect.AbstractIterator;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import org.apache.commons.io.input.ProxyInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JarScanner extends AbstractIterator<InputStream> {
   private static final Logger log = LoggerFactory.getLogger(JarScanner.class);
   private final String url;
   private JarInputStream inputStream;

   public JarScanner(String url) {
      log.debug("Initializing jar scanner with URL {}", url);
      this.url = url;
   }

   protected InputStream computeNext() {
      if(null == this.inputStream) {
         try {
            this.inputStream = new JarInputStream((new URL(this.url)).openStream());
         } catch (IOException var2) {
            log.error("Error opening jar file " + this.url, var2);
            return (InputStream)this.endOfData();
         }
      }

      while(true) {
         try {
            JarEntry nextEntry = this.inputStream.getNextJarEntry();
            if(nextEntry == null) {
               return (InputStream)this.endOfData();
            }

            if(!nextEntry.isDirectory() && nextEntry.getName().endsWith(".class")) {
               return new ProxyInputStream(this.inputStream) {
                  public void close() throws IOException {
                     JarScanner.this.inputStream.closeEntry();
                  }
               };
            }
         } catch (IOException var3) {
            log.error("Error reading entry from jar file", var3);
            return (InputStream)this.endOfData();
         }
      }
   }
}
