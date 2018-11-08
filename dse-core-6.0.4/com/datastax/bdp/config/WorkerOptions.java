package com.datastax.bdp.config;

import java.util.HashSet;
import java.util.Set;

public class WorkerOptions {
   public String cores_total = "0.7";
   public String memory_total = "0.6";
   public Set<WorkPoolOptions> workpools = new HashSet();

   public WorkerOptions() {
   }
}
