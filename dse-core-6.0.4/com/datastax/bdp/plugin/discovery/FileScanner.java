package com.datastax.bdp.plugin.discovery;

import com.google.common.collect.AbstractIterator;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileScanner extends AbstractIterator<InputStream> {
   private static final Logger log = LoggerFactory.getLogger(FileScanner.class);
   private Iterator<File> files;
   private JarScanner jarScanner;

   public FileScanner(String filePath) {
      log.debug("Initializing directory scanner with root {}", filePath);
      this.files = FileUtils.iterateFiles(new File(filePath), new String[]{"class", "jar"}, true);
   }

   protected InputStream computeNext() {
      if(this.jarScanner != null) {
         if(this.jarScanner.hasNext()) {
            return (InputStream)this.jarScanner.next();
         } else {
            this.jarScanner = null;
            return this.computeNext();
         }
      } else if(this.files.hasNext()) {
         File f = (File)this.files.next();
         if(f.getName().endsWith(".jar")) {
            this.jarScanner = new JarScanner("file:" + f.getAbsolutePath());
            return this.computeNext();
         } else {
            return this.getInputStream(f);
         }
      } else {
         return (InputStream)this.endOfData();
      }
   }

   private InputStream getInputStream(File file) {
      try {
         return FileUtils.openInputStream(file);
      } catch (IOException var3) {
         log.error("Unable to open file {} when scanning for DSE plugins", file, var3);
         return (InputStream)this.endOfData();
      }
   }
}
