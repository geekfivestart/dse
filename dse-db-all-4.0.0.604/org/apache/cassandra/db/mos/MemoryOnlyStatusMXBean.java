package org.apache.cassandra.db.mos;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.List;

public interface MemoryOnlyStatusMXBean {
   String MXBEAN_NAME = "org.apache.cassandra.db:type=MemoryOnlyStatus";

   List<MemoryOnlyStatusMXBean.TableInfo> getMemoryOnlyTableInformation();

   MemoryOnlyStatusMXBean.TableInfo getMemoryOnlyTableInformation(String var1, String var2);

   MemoryOnlyStatusMXBean.TotalInfo getMemoryOnlyTotals();

   double getMemoryOnlyPercentUsed();

   public static class TotalInfo implements Serializable {
      private final long used;
      private final long notAbleToLock;
      private final long maxMemoryToLock;

      @ConstructorProperties({"used", "notAbleToLock", "maxMemoryToLock"})
      public TotalInfo(long used, long notAbleToLock, long maxMemoryToLock) {
         this.used = used;
         this.notAbleToLock = notAbleToLock;
         this.maxMemoryToLock = maxMemoryToLock;
      }

      public long getUsed() {
         return this.used;
      }

      public long getNotAbleToLock() {
         return this.notAbleToLock;
      }

      public long getMaxMemoryToLock() {
         return this.maxMemoryToLock;
      }
   }

   public static class TableInfo implements Serializable {
      private final String ks;
      private final String cf;
      private final long used;
      private final long notAbleToLock;
      private final long maxMemoryToLock;

      @ConstructorProperties({"ks", "cf", "used", "notAbleToLock", "maxMemoryToLock"})
      public TableInfo(String ks, String cf, long used, long notAbleToLock, long maxMemoryToLock) {
         this.ks = ks;
         this.cf = cf;
         this.used = used;
         this.notAbleToLock = notAbleToLock;
         this.maxMemoryToLock = maxMemoryToLock;
      }

      public String getKs() {
         return this.ks;
      }

      public String getCf() {
         return this.cf;
      }

      public long getUsed() {
         return this.used;
      }

      public long getNotAbleToLock() {
         return this.notAbleToLock;
      }

      public long getMaxMemoryToLock() {
         return this.maxMemoryToLock;
      }
   }
}
