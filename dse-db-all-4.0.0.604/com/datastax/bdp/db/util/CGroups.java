package com.datastax.bdp.db.util;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.BitSet;

public final class CGroups {
   public static long MEM_UNLIMITED = 4611686018427387904L;

   public CGroups() {
   }

   static String controllerGroup(String procCGroup, String controller) {
      if(procCGroup != null && controller != null) {
         String[] var2 = procCGroup.split("\n");
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String cgroupLine = var2[var4];
            String[] parts = cgroupLine.split(":");
            if(parts.length >= 3) {
               String[] controllers = parts[1].split(",");
               String[] var8 = controllers;
               int var9 = controllers.length;

               for(int var10 = 0; var10 < var9; ++var10) {
                  String ctl = var8[var10];
                  if(controller.equals(ctl)) {
                     return parts[2].isEmpty()?"/":parts[2];
                  }
               }
            }
         }

         return "/";
      } else {
         return "/";
      }
   }

   private static String procCGroupsFileContents() {
      File procCGroups = new File("/proc/self/cgroup");
      return !procCGroups.isFile()?null:readFile(procCGroups);
   }

   private static String readFile(File f) {
      if(f == null) {
         return null;
      } else {
         try {
            return new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
         } catch (IOException var2) {
            return null;
         }
      }
   }

   static File controllerFile(String procCGroup, String controller, String file) {
      if(file == null) {
         return null;
      } else {
         String cgroupPart = controllerGroup(procCGroup, controller);
         if(!cgroupPart.endsWith("/")) {
            cgroupPart = cgroupPart + "/";
         }

         return new File("/sys/fs/cgroup/" + controller + cgroupPart + file);
      }
   }

   static Integer countCpus(String cpus) {
      if(cpus != null && !cpus.isEmpty()) {
         BitSet bits = new BitSet();

         try {
            String[] var2 = cpus.split(",");
            int var3 = var2.length;

            for(int var4 = 0; var4 < var3; ++var4) {
               String elem = var2[var4];
               elem = elem.trim();
               if(!elem.isEmpty()) {
                  if(elem.indexOf(45) != -1) {
                     String[] pair = elem.split("-");
                     int start = Integer.parseInt(pair[0].trim());
                     int end = Integer.parseInt(pair[1].trim());

                     for(int n = start; n <= end; ++n) {
                        bits.set(n);
                     }
                  } else {
                     bits.set(Integer.parseInt(elem));
                  }
               }
            }

            return Integer.valueOf(bits.cardinality());
         } catch (Exception var10) {
            return null;
         }
      } else {
         return null;
      }
   }

   public static Integer countCpus() {
      File f = controllerFile(procCGroupsFileContents(), "cpuset", "cpuset.cpus");
      String cpus = readFile(f);
      return countCpus(cpus);
   }

   public static long memoryLimit() {
      String procCGroup = procCGroupsFileContents();
      long softLimit = longValue(controllerFile(procCGroup, "memory", "memory.soft_limit_in_bytes"));
      long limit = longValue(controllerFile(procCGroup, "memory", "memory.limit_in_bytes"));
      return Math.min(softLimit, limit);
   }

   static long longValue(File f) {
      return longValue(readFile(f));
   }

   static long longValue(String s) {
      if(s == null) {
         return MEM_UNLIMITED;
      } else {
         long v = 9223372036854775807L;

         try {
            BigInteger val = new BigInteger(s);
            if(val.compareTo(BigInteger.valueOf(9223372036854775807L)) <= 0 && val.compareTo(BigInteger.ZERO) > 0) {
               v = val.longValue();
            }
         } catch (Exception var5) {
            ;
         }

         return v >= MEM_UNLIMITED?MEM_UNLIMITED:v;
      }
   }

   public static boolean blkioThrottled() {
      String procCGroup = procCGroupsFileContents();
      return notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.read_iops_device")) || notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.read_bps_device")) || notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.write_iops_device")) || notEmpty(controllerFile(procCGroup, "blkio", "blkio.throttle.write_bps_device"));
   }

   private static boolean notEmpty(File file) {
      String s = readFile(file);
      return s != null && !s.isEmpty();
   }
}
