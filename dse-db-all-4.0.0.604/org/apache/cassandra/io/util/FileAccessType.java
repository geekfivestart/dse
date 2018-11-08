package org.apache.cassandra.io.util;

public enum FileAccessType {
   RANDOM,
   SEQUENTIAL,
   FULL_FILE;

   private FileAccessType() {
   }
}
