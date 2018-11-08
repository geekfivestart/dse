package org.apache.cassandra.utils;

import org.hyperic.sigar.FileSystemMap;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SigarLibrary {
   private Logger logger = LoggerFactory.getLogger(SigarLibrary.class);
   public static final SigarLibrary instance = new SigarLibrary();
   private Sigar sigar;
   private FileSystemMap mounts = null;
   private boolean initialized = false;
   private long INFINITY = -1L;
   private long EXPECTED_MIN_NOFILE = 10000L;
   private long EXPECTED_NPROC = 32768L;
   private long EXPECTED_AS;

   private SigarLibrary() {
      this.EXPECTED_AS = this.INFINITY;
      this.logger.info("Initializing SIGAR library");

      try {
         this.sigar = new Sigar();
         this.mounts = this.sigar.getFileSystemMap();
         this.initialized = true;
      } catch (SigarException var2) {
         this.logger.info("Could not initialize SIGAR library {} ", var2.getMessage());
      } catch (UnsatisfiedLinkError var3) {
         this.logger.info("Could not initialize SIGAR library {} ", var3.getMessage());
      }

   }

   public boolean initialized() {
      return this.initialized;
   }

   private boolean hasAcceptableProcNumber() {
      try {
         long fileMax = this.sigar.getResourceLimit().getProcessesMax();
         return fileMax >= this.EXPECTED_NPROC || fileMax == this.INFINITY;
      } catch (SigarException var3) {
         this.logger.warn("Could not determine if max processes was acceptable. Error message: {}", var3);
         return false;
      }
   }

   private boolean hasAcceptableFileLimits() {
      try {
         long fileMax = this.sigar.getResourceLimit().getOpenFilesMax();
         return fileMax >= this.EXPECTED_MIN_NOFILE || fileMax == this.INFINITY;
      } catch (SigarException var3) {
         this.logger.warn("Could not determine if max open file handle limit is correctly configured. Error message: {}", var3);
         return false;
      }
   }

   private boolean hasAcceptableAddressSpace() {
      if(FBUtilities.isWindows) {
         return true;
      } else {
         try {
            long fileMax = this.sigar.getResourceLimit().getVirtualMemoryMax();
            return fileMax == this.EXPECTED_AS;
         } catch (SigarException var3) {
            this.logger.warn("Could not determine if VirtualMemoryMax was acceptable. Error message: {}", var3);
            return false;
         }
      }
   }

   private boolean isSwapEnabled() {
      try {
         Swap swap = this.sigar.getSwap();
         long swapSize = swap.getTotal();
         return swapSize > 0L;
      } catch (SigarException var4) {
         this.logger.warn("Could not determine if swap configuration is acceptable. Error message: {}", var4);
         return false;
      }
   }

   public long getPid() {
      return this.initialized?this.sigar.getPid():-1L;
   }

   public void warnIfRunningInDegradedMode() {
      if(this.initialized) {
         boolean swapEnabled = this.isSwapEnabled();
         boolean goodAddressSpace = this.hasAcceptableAddressSpace();
         boolean goodFileLimits = this.hasAcceptableFileLimits();
         boolean goodProcNumber = this.hasAcceptableProcNumber();
         if(!swapEnabled && goodAddressSpace && goodFileLimits && goodProcNumber) {
            this.logger.info("Checked OS settings and found them configured for optimal performance.");
         } else {
            this.logger.warn("DSE server running in degraded mode. Is swap disabled? : {},  Address space adequate? : {},  nofile limit adequate? : {}, nproc limit adequate? : {} ", new Object[]{Boolean.valueOf(!swapEnabled), Boolean.valueOf(goodAddressSpace), Boolean.valueOf(goodFileLimits), Boolean.valueOf(goodProcNumber)});
         }
      } else {
         this.logger.info("Sigar could not be initialized, test for checking degraded mode omitted.");
      }

   }
}
