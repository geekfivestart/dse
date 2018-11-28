package com.datastax.bdp.util;

import com.diffplug.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.hyperic.sigar.ProcState;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SigarUtil {
   private static final Logger logger = LoggerFactory.getLogger(SigarUtil.class);
   public static final Supplier<Sigar> sigar = Suppliers.memoize(Sigar::new);

   public SigarUtil() {
   }

   public static void killProcessTree(long processId, int signal) {
      traverseProcessTree(Long.valueOf(processId), (pid) -> {
         kill(pid.longValue(), signal);
      });
   }

   public static LambdaMayThrow.SupplierMayThrow<Set<Long>> liveSubprocesses(Process process) {
      long pid = (long)getProcessID(process);
      Set<Long> appPids = findProcesses(pid, (p) -> {
         return true;
      });
      return () -> {
         Set<Long> livePids = (Set)Arrays.stream(((Sigar)sigar.get()).getProcList()).boxed().collect(Collectors.toSet());
         livePids.retainAll(appPids);
         return livePids;
      };
   }

   public static Set<Long> findProcesses(long rootProcessId, Predicate<Long> filter) {
      Set<Long> collectedProcesses = Sets.newHashSet();
      traverseProcessTree(Long.valueOf(rootProcessId), (pid) -> {
         if(filter.test(pid)) {
            collectedProcesses.add(pid);
         }

      });
      return collectedProcesses;
   }

   public static void kill(long processId, int signal) {
      logger.info("Killing process {} with signal {}", Long.valueOf(processId), Integer.valueOf(signal));

      try {
         ((Sigar)sigar.get()).kill(processId, signal);
      } catch (Throwable var4) {
         logger.warn("Could not kill process {}: {}", Long.valueOf(processId), var4.getMessage());
      }

   }

   public static int getProcessID(Process process) {
      if(SystemUtils.IS_OS_UNIX) {
         return getUnixProcessID(process);
      } else {
         throw new UnsupportedOperationException("This method is not supported on this operation system");
      }
   }

   public static long[] getUserProcesses(UserPrincipal user) throws SigarException {
      try {
         return Arrays.stream(((Sigar)sigar.get()).getProcList()).filter((pid) -> {
            try {
               return Objects.equals(((Sigar)sigar.get()).getProcCredName(pid).getUser(), user.getName());
            } catch (SigarException var4) {
               throw new RuntimeException(var4);
            }
         }).toArray();
      } catch (RuntimeException var3) {
         Optional<Throwable> sigarCause = DseUtil.findCause(var3, SigarException.class);
         if(sigarCause.isPresent()) {
            throw (SigarException)sigarCause.get();
         } else {
            throw var3;
         }
      }
   }

   private static int getUnixProcessID(Process process) {
      if(!SystemUtils.IS_OS_UNIX) {
         throw new UnsupportedOperationException("This method can be run only on Unix-like OS");
      } else {
         try {
            Class cls = Class.forName("java.lang.UNIXProcess");
            Field field = FieldUtils.getDeclaredField(cls, "pid", true);
            return field.getInt(process);
         } catch (Exception var3) {
            throw new RuntimeException("Failed to get PID of a process " + process, var3);
         }
      }
   }

   private static Multimap<Long, Long> getPpidToPidMap() {
      HashMultimap result = HashMultimap.create();

      try {
         long[] procList = ((Sigar)sigar.get()).getProcList();
         long[] var2 = procList;
         int var3 = procList.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            long procId = var2[var4];

            try {
               ProcState procStat = ((Sigar)sigar.get()).getProcState(procId);
               if(procStat != null) {
                  result.put(Long.valueOf(procStat.getPpid()), Long.valueOf(procId));
               }
            } catch (Throwable var8) {
               logger.warn("Failed to get process {} stat: {}", Long.valueOf(procId), var8.getMessage());
            }
         }

         return result;
      } catch (Throwable var9) {
         logger.warn("Failed to get processes map: {}", var9);
         return result;
      }
   }

   private static void traverseProcessTree(Long rootPid, Consumer<Long> action) {
      Queue<Long> queue = new LinkedList();
      queue.add(rootPid);

      long nextPid;
      for(Multimap processesMap = getPpidToPidMap(); !queue.isEmpty(); action.accept(Long.valueOf(nextPid))) {
         nextPid = ((Long)queue.remove()).longValue();
         Collection<Long> childrenPids = processesMap.get(Long.valueOf(nextPid));
         if(childrenPids != null) {
            queue.addAll(childrenPids);
         }
      }

   }
}
