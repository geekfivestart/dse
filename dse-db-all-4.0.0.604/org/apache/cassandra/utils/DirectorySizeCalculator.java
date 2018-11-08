package org.apache.cassandra.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class DirectorySizeCalculator extends SimpleFileVisitor<Path> {
   protected volatile long size = 0L;
   protected final File path;

   public DirectorySizeCalculator(File path) {
      this.path = path;
   }

   public boolean isAcceptable(Path file) {
      return true;
   }

   public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      if(this.isAcceptable(file)) {
         this.size += attrs.size();
      }

      return FileVisitResult.CONTINUE;
   }

   public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      return FileVisitResult.CONTINUE;
   }

   public long getAllocatedSize() {
      return this.size;
   }
}
