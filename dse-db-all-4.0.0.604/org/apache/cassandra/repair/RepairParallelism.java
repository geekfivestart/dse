package org.apache.cassandra.repair;

public enum RepairParallelism {
   SEQUENTIAL("sequential"),
   PARALLEL("parallel"),
   DATACENTER_AWARE("dc_parallel");

   private final String name;

   public static RepairParallelism fromName(String name) {
      return PARALLEL.getName().equals(name)?PARALLEL:(DATACENTER_AWARE.getName().equals(name)?DATACENTER_AWARE:SEQUENTIAL);
   }

   private RepairParallelism(String name) {
      this.name = name;
   }

   public String getName() {
      return this.name;
   }

   public String toString() {
      return this.getName();
   }
}
