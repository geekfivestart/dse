package com.datastax.bdp.util;

import com.sun.jna.Native;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemUtil {
   private static final Logger logger = LoggerFactory.getLogger(FileSystemUtil.class);
   public static final boolean enabled = tryRegister();

   public FileSystemUtil() {
   }

   private static boolean tryRegister() {
      try {
         Native.register("c");
         File jnaDir = new File(System.getProperty("java.io.tmpdir"), "jna");
         chmod(jnaDir.getAbsolutePath(), 511);
         return true;
      } catch (NoClassDefFoundError var1) {
         logger.warn("JNA failed to register native C library: " + var1.getMessage());
         return false;
      } catch (UnsatisfiedLinkError var2) {
         logger.warn("JNA failed to register native C library: " + var2.getMessage());
         return false;
      } catch (NoSuchMethodError var3) {
         logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
         return false;
      } catch (Exception var4) {
         logger.warn("JNA failed to register native C library: " + var4.getMessage());
         return false;
      }
   }

   public static boolean initJna() {
      return enabled;
   }

   public static native int chmod(String var0, int var1);

   public static FileSystemUtil.FileStatus getFileStatus(String file) throws IOException {
      String[] args = new String[]{"ls", "-ld", file};
      String output = Shell.execCommand(args);
      StringTokenizer t = new StringTokenizer(output);
      String permissions = t.nextToken();
      if(permissions.length() > 10) {
         permissions = permissions.substring(0, 10);
      }

      t.nextToken();
      String owner = t.nextToken();
      String group = t.nextToken();
      return new FileSystemUtil.FileStatus(permissions, owner, group);
   }

   public static File findDeepestExistingFileOrDir(File file) {
      return file.exists()?file:findDeepestExistingFileOrDir(file.getParentFile());
   }

   public static boolean isAccessibleDirectory(File file) {
      return file.isDirectory() && file.canRead() && file.canWrite() && file.canExecute();
   }

   public static class FileStatus {
      public final int permissions;
      public final String owner;
      public final String group;

      private FileStatus(String permissions, String owner, String group) {
         this.permissions = this.toShort(permissions);
         this.owner = owner;
         this.group = group;
      }

      private int toShort(String permissions) {
         boolean setuid = false;
         boolean setgid = false;
         int n = 0;

         for(int i = 1; i < permissions.length(); ++i) {
            n <<= 1;
            char c = permissions.charAt(i);
            n += c != 45 && c != 84 && c != 83?1:0;
            if(c == 115 && i == 3) {
               setuid = true;
            }

            if(c == 115 && i == 6) {
               setgid = true;
            }
         }

         if(setuid) {
            n += 2048;
         }

         if(setgid) {
            n += 1024;
         }

         return n;
      }
   }
}
