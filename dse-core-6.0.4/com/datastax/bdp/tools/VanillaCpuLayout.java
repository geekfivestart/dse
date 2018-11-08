package com.datastax.bdp.tools;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class VanillaCpuLayout {
   public static final int MAX_CPUS_SUPPORTED = 64;
   @NotNull
   private final List<VanillaCpuLayout.CpuInfo> cpuDetails;
   private final int sockets;
   private final int coresPerSocket;
   private final int threadsPerCore;

   @VisibleForTesting
   public VanillaCpuLayout(@NotNull List<VanillaCpuLayout.CpuInfo> cpuDetails) {
      this.cpuDetails = cpuDetails;
      SortedSet<Integer> sockets = new TreeSet();
      SortedSet<Integer> cores = new TreeSet();
      SortedSet<Integer> threads = new TreeSet();
      Iterator var5 = cpuDetails.iterator();

      while(var5.hasNext()) {
         VanillaCpuLayout.CpuInfo cpuDetail = (VanillaCpuLayout.CpuInfo)var5.next();
         sockets.add(Integer.valueOf(cpuDetail.socketId));
         cores.add(Integer.valueOf((cpuDetail.socketId << 16) + cpuDetail.coreId));
         threads.add(Integer.valueOf(cpuDetail.threadId));
      }

      this.sockets = sockets.size();
      this.coresPerSocket = cores.size() / sockets.size();
      this.threadsPerCore = threads.size();
      if(cpuDetails.size() != this.sockets() * this.coresPerSocket() * this.threadsPerCore()) {
         StringBuilder error = new StringBuilder();
         error.append("cpuDetails.size= ").append(cpuDetails.size()).append(" != sockets: ").append(this.sockets()).append(" * coresPerSocket: ").append(this.coresPerSocket()).append(" * threadsPerCore: ").append(this.threadsPerCore()).append('\n');
         Iterator var9 = cpuDetails.iterator();

         while(var9.hasNext()) {
            VanillaCpuLayout.CpuInfo detail = (VanillaCpuLayout.CpuInfo)var9.next();
            error.append(detail).append('\n');
         }

         throw new AssertionError(error);
      }
   }

   @NotNull
   public static VanillaCpuLayout fromProperties(String fileName) throws IOException {
      return fromProperties(openFile(fileName));
   }

   @NotNull
   public static VanillaCpuLayout fromProperties(InputStream is) throws IOException {
      Properties prop = new Properties();
      prop.load(is);
      return fromProperties(prop);
   }

   @NotNull
   public static VanillaCpuLayout fromProperties(@NotNull Properties prop) {
      List<VanillaCpuLayout.CpuInfo> cpuDetails = new ArrayList();

      for(int i = 0; i < 64; ++i) {
         String line = prop.getProperty("" + i);
         if(line == null) {
            break;
         }

         String[] word = line.trim().split(" *, *");
         VanillaCpuLayout.CpuInfo details = new VanillaCpuLayout.CpuInfo(Integer.parseInt(word[0]), Integer.parseInt(word[1]), Integer.parseInt(word[2]));
         cpuDetails.add(details);
      }

      return new VanillaCpuLayout(cpuDetails);
   }

   @NotNull
   public static VanillaCpuLayout fromCpuInfo() throws IOException {
      return fromCpuInfo("/proc/cpuinfo");
   }

   @NotNull
   public static VanillaCpuLayout fromCpuInfo(String filename) throws IOException {
      return fromCpuInfo(openFile(filename));
   }

   private static InputStream openFile(String filename) throws FileNotFoundException {
      try {
         return new FileInputStream(filename);
      } catch (FileNotFoundException var3) {
         InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
         if(is == null) {
            throw var3;
         } else {
            return is;
         }
      }
   }

   @NotNull
   public static VanillaCpuLayout fromCpuInfo(InputStream is) throws IOException {
      BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
      List<VanillaCpuLayout.CpuInfo> cpuDetails = new ArrayList();
      VanillaCpuLayout.CpuInfo details = new VanillaCpuLayout.CpuInfo();
      LinkedHashMap threadCount = new LinkedHashMap();

      String line;
      while((line = br.readLine()) != null) {
         if(line.trim().isEmpty()) {
            String key = details.socketId + "," + details.coreId;
            Integer count = (Integer)threadCount.get(key);
            if(count == null) {
               threadCount.put(key, count = Integer.valueOf(1));
            } else {
               threadCount.put(key, count = Integer.valueOf(count.intValue() + 1));
            }

            details.threadId = count.intValue() - 1;
            cpuDetails.add(details);
            details = new VanillaCpuLayout.CpuInfo();
            details.coreId = cpuDetails.size();
         } else {
            String[] words = line.split("\\s*:\\s*", 2);
            if(words[0].equals("physical id")) {
               details.socketId = Integer.parseInt(words[1]);
            } else if(words[0].equals("core id")) {
               details.coreId = Integer.parseInt(words[1]);
            }
         }
      }

      return new VanillaCpuLayout(cpuDetails);
   }

   public int cpus() {
      return this.cpuDetails.size();
   }

   public int sockets() {
      return this.sockets;
   }

   public int coresPerSocket() {
      return this.coresPerSocket;
   }

   public int threadsPerCore() {
      return this.threadsPerCore;
   }

   public int socketId(int cpuId) {
      return ((VanillaCpuLayout.CpuInfo)this.cpuDetails.get(cpuId)).socketId;
   }

   public int coreId(int cpuId) {
      return ((VanillaCpuLayout.CpuInfo)this.cpuDetails.get(cpuId)).coreId;
   }

   public int threadId(int cpuId) {
      return ((VanillaCpuLayout.CpuInfo)this.cpuDetails.get(cpuId)).threadId;
   }

   @NotNull
   public String toString() {
      StringBuilder sb = new StringBuilder();
      int i = 0;

      for(int cpuDetailsSize = this.cpuDetails.size(); i < cpuDetailsSize; ++i) {
         VanillaCpuLayout.CpuInfo cpuDetail = (VanillaCpuLayout.CpuInfo)this.cpuDetails.get(i);
         sb.append(i).append(": ").append(cpuDetail).append('\n');
      }

      return sb.toString();
   }

   public boolean equals(@Nullable Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         VanillaCpuLayout that = (VanillaCpuLayout)o;
         return this.coresPerSocket != that.coresPerSocket?false:(this.sockets != that.sockets?false:(this.threadsPerCore != that.threadsPerCore?false:this.cpuDetails.equals(that.cpuDetails)));
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.cpuDetails.hashCode();
      result = 31 * result + this.sockets;
      result = 31 * result + this.coresPerSocket;
      result = 31 * result + this.threadsPerCore;
      return result;
   }

   public static class CpuInfo {
      int socketId;
      int coreId;
      int threadId;

      CpuInfo() {
      }

      @VisibleForTesting
      public CpuInfo(int socketId, int coreId, int threadId) {
         this.socketId = socketId;
         this.coreId = coreId;
         this.threadId = threadId;
      }

      @NotNull
      public String toString() {
         return "CpuInfo{socketId=" + this.socketId + ", coreId=" + this.coreId + ", threadId=" + this.threadId + '}';
      }

      public boolean equals(@Nullable Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            VanillaCpuLayout.CpuInfo cpuInfo = (VanillaCpuLayout.CpuInfo)o;
            return this.coreId != cpuInfo.coreId?false:(this.socketId != cpuInfo.socketId?false:this.threadId == cpuInfo.threadId);
         } else {
            return false;
         }
      }

      public int hashCode() {
         int result = this.socketId;
         result = 31 * result + this.coreId;
         result = 31 * result + this.threadId;
         return result;
      }
   }
}
