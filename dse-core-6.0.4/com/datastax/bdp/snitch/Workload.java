package com.datastax.bdp.snitch;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum Workload {
   Unknown,
   Analytics,
   Cassandra,
   Search,
   Graph;

   private static Logger logger = LoggerFactory.getLogger(Workload.class);
   private static final Comparator<String> COMP = String::compareTo;

   private Workload() {
   }

   public boolean isCompatibleWith(Set<Workload> workloads) {
      return null != workloads && workloads.contains(this);
   }

   public static Set<String> toStringSet(Set<Workload> workloads) {
      Set<String> workloadNames = new HashSet();
      if(!isDefined(workloads)) {
         workloadNames.add(Unknown.name());
         return workloadNames;
      } else {
         workloadNames.addAll((Collection)workloads.stream().map((w) -> {
            return w.name();
         }).collect(Collectors.toList()));
         return Collections.unmodifiableSet(workloadNames);
      }
   }

   public static Set<Workload> fromStringSet(Set<String> workloadNames, InetAddress endpoint) {
      Set<Workload> workloads = fromStringSet(workloadNames);
      if(workloads.contains(Unknown)) {
         logger.warn("Couldn't determine workloads for {} from value {}", endpoint, workloadNames == null?"NULL":workloads);
      }

      return Collections.unmodifiableSet(workloads);
   }

   public static Set<Workload> fromStringSet(Set<String> workloadNames) {
      Set<Workload> workloads = EnumSet.noneOf(Workload.class);
      if(null != workloadNames && !workloadNames.isEmpty()) {
         try {
            workloads.addAll((Collection)workloadNames.stream().map(Workload::valueOf).collect(Collectors.toList()));
         } catch (Exception var3) {
            workloads.add(Unknown);
         }

         return Collections.unmodifiableSet(workloads);
      } else {
         workloads.add(Unknown);
         return workloads;
      }
   }

   public static Set<Workload> fromString(String workloadNames, InetAddress endpoint) {
      Set<Workload> workloads = fromString(workloadNames);
      if(workloads.contains(Unknown)) {
         logger.warn("Couldn't determine workloads for {} from value {}", endpoint, workloadNames == null?"NULL":workloads);
      }

      return Collections.unmodifiableSet(workloads);
   }

   public static Set<Workload> fromString(String workloadNames) {
      EnumSet workloads = EnumSet.noneOf(Workload.class);

      try {
         Arrays.stream(values()).filter((w) -> {
            return workloadNames.contains(w.name());
         }).forEach(workloads::add);
      } catch (Exception var3) {
         ;
      }

      if(workloads.isEmpty()) {
         workloads.add(Unknown);
      }

      return Collections.unmodifiableSet(workloads);
   }

   public static String workloadNames(Set<Workload> workloads) {
      return !isDefined(workloads)?Unknown.name():(String)workloads.stream().map((w) -> {
         return w.name();
      }).sorted(COMP.reversed()).collect(Collectors.joining());
   }

   public static String legacyWorkloadName(Set<Workload> workloads) {
      if(!isDefined(workloads)) {
         return Unknown.name();
      } else {
         String legacyWorkload = workloadNames(workloads).replace(Graph.name(), "");
         legacyWorkload = !legacyWorkload.equals(Cassandra.name())?legacyWorkload.replace(Cassandra.name(), ""):legacyWorkload;
         return "".equals(legacyWorkload)?Unknown.name():legacyWorkload;
      }
   }

   public static boolean isDefined(Set<Workload> workloads) {
      return null != workloads && !workloads.isEmpty();
   }

   public static Set<Workload> fromLegacyWorkloadName(String legacyWorkload, boolean graphEnabled) {
      if(null != legacyWorkload && !legacyWorkload.isEmpty() && !Unknown.name().equals(legacyWorkload)) {
         Set<Workload> workloads = EnumSet.of(Cassandra);
         if(!"Cassandra".equals(legacyWorkload)) {
            if("Search".equals(legacyWorkload)) {
               workloads.add(Search);
            } else if("Analytics".equals(legacyWorkload)) {
               workloads.add(Analytics);
            } else {
               if(!"SearchAnalytics".equals(legacyWorkload)) {
                  return Collections.unmodifiableSet(EnumSet.of(Unknown));
               }

               workloads.add(Search);
               workloads.add(Analytics);
            }
         }

         if(graphEnabled) {
            workloads.add(Graph);
         }

         return Collections.unmodifiableSet(workloads);
      } else {
         return Collections.unmodifiableSet(EnumSet.of(Unknown));
      }
   }
}
