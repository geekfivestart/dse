package org.apache.cassandra.repair.messages;

import com.google.common.base.Joiner;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairOption {
   public static final String PARALLELISM_KEY = "parallelism";
   public static final String PRIMARY_RANGE_KEY = "primaryRange";
   public static final String INCREMENTAL_KEY = "incremental";
   public static final String JOB_THREADS_KEY = "jobThreads";
   public static final String RANGES_KEY = "ranges";
   public static final String COLUMNFAMILIES_KEY = "columnFamilies";
   public static final String DATACENTERS_KEY = "dataCenters";
   public static final String HOSTS_KEY = "hosts";
   public static final String TRACE_KEY = "trace";
   public static final String SUB_RANGE_REPAIR_KEY = "sub_range_repair";
   public static final String PULL_REPAIR_KEY = "pullRepair";
   public static final String FORCE_REPAIR_KEY = "forceRepair";
   public static final String PREVIEW = "previewKind";
   public static final int MAX_JOB_THREADS = 4;
   private static final Logger logger = LoggerFactory.getLogger(RepairOption.class);
   private final RepairParallelism parallelism;
   private final boolean primaryRange;
   private final boolean incremental;
   private final boolean trace;
   private final int jobThreads;
   private final boolean pullRepair;
   private final boolean forceRepair;
   private final PreviewKind previewKind;
   private final Collection<String> columnFamilies = new HashSet();
   private final Collection<String> dataCenters = new HashSet();
   private final Collection<String> hosts = new HashSet();
   private final Collection<Range<Token>> ranges = new HashSet();

   public static RepairOption parse(Map<String, String> options, IPartitioner partitioner) {
      RepairParallelism parallelism = RepairParallelism.fromName((String)options.get("parallelism"));
      boolean primaryRange = Boolean.parseBoolean((String)options.get("primaryRange"));
      boolean incremental = Boolean.parseBoolean((String)options.get("incremental"));
      PreviewKind previewKind = PreviewKind.valueOf((String)options.getOrDefault("previewKind", PreviewKind.NONE.toString()));
      boolean trace = Boolean.parseBoolean((String)options.get("trace"));
      boolean force = Boolean.parseBoolean((String)options.get("forceRepair"));
      boolean pullRepair = Boolean.parseBoolean((String)options.get("pullRepair"));
      int jobThreads = 1;
      if(options.containsKey("jobThreads")) {
         try {
            jobThreads = Integer.parseInt((String)options.get("jobThreads"));
         } catch (NumberFormatException var20) {
            ;
         }
      }

      String rangesStr = (String)options.get("ranges");
      Set<Range<Token>> ranges = new HashSet();
      if(rangesStr != null) {
         StringTokenizer tokenizer = new StringTokenizer(rangesStr, ",");

         while(tokenizer.hasMoreTokens()) {
            String[] rangeStr = tokenizer.nextToken().split(":", 2);
            if(rangeStr.length >= 2) {
               Token parsedBeginToken = partitioner.getTokenFactory().fromString(rangeStr[0].trim());
               Token parsedEndToken = partitioner.getTokenFactory().fromString(rangeStr[1].trim());
               if(parsedBeginToken.equals(parsedEndToken)) {
                  throw new IllegalArgumentException("Start and end tokens must be different.");
               }

               ranges.add(new Range(parsedBeginToken, parsedEndToken));
            }
         }
      }

      RepairOption option = new RepairOption(parallelism, primaryRange, incremental, trace, jobThreads, ranges, pullRepair, force, previewKind);
      String dataCentersStr = (String)options.get("dataCenters");
      Collection<String> dataCenters = new HashSet();
      if(dataCentersStr != null) {
         StringTokenizer tokenizer = new StringTokenizer(dataCentersStr, ",");

         while(tokenizer.hasMoreTokens()) {
            dataCenters.add(tokenizer.nextToken().trim());
         }

         option.getDataCenters().addAll(dataCenters);
      }

      String hostsStr = (String)options.get("hosts");
      Collection<String> hosts = new HashSet();
      if(hostsStr != null) {
         StringTokenizer tokenizer = new StringTokenizer(hostsStr, ",");

         while(tokenizer.hasMoreTokens()) {
            hosts.add(tokenizer.nextToken().trim());
         }

         option.getHosts().addAll(hosts);
      }

      String cfStr = (String)options.get("columnFamilies");
      if(cfStr != null) {
         Collection<String> columnFamilies = new HashSet();
         StringTokenizer tokenizer = new StringTokenizer(cfStr, ",");

         while(tokenizer.hasMoreTokens()) {
            columnFamilies.add(tokenizer.nextToken().trim());
         }

         option.getColumnFamilies().addAll(columnFamilies);
      }

      if(jobThreads > 4) {
         throw new IllegalArgumentException("Too many job threads. Max is 4");
      } else if(!dataCenters.isEmpty() && !hosts.isEmpty()) {
         throw new IllegalArgumentException("Cannot combine -dc and -hosts options.");
      } else if(!primaryRange || (dataCenters.isEmpty() || option.isInLocalDCOnly()) && hosts.isEmpty()) {
         if(pullRepair) {
            if(hosts.size() != 2) {
               throw new IllegalArgumentException("Pull repair can only be performed between two hosts. Please specify two hosts, one of which must be this host.");
            }

            if(ranges.isEmpty()) {
               throw new IllegalArgumentException("Token ranges must be specified when performing pull repair. Please specify at least one token range which both hosts have in common.");
            }
         }

         return option;
      } else {
         throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
      }
   }

   public RepairOption(RepairParallelism parallelism, boolean primaryRange, boolean incremental, boolean trace, int jobThreads, Collection<Range<Token>> ranges, boolean pullRepair, boolean forceRepair, PreviewKind previewKind) {
      if(FBUtilities.isWindows && (DatabaseDescriptor.getDiskAccessMode() != Config.AccessMode.standard || DatabaseDescriptor.getIndexAccessMode() != Config.AccessMode.standard) && parallelism == RepairParallelism.SEQUENTIAL) {
         logger.warn("Sequential repair disabled when memory-mapped I/O is configured on Windows. Reverting to parallel.");
         this.parallelism = RepairParallelism.PARALLEL;
      } else {
         this.parallelism = parallelism;
      }

      this.primaryRange = primaryRange;
      this.incremental = incremental;
      this.trace = trace;
      this.jobThreads = jobThreads;
      this.ranges.addAll(ranges);
      this.pullRepair = pullRepair;
      this.forceRepair = forceRepair;
      this.previewKind = previewKind;
   }

   public RepairParallelism getParallelism() {
      return this.parallelism;
   }

   public boolean isPrimaryRange() {
      return this.primaryRange;
   }

   public boolean isIncremental() {
      return this.incremental;
   }

   public boolean isTraced() {
      return this.trace;
   }

   public boolean isPullRepair() {
      return this.pullRepair;
   }

   public boolean isForcedRepair() {
      return this.forceRepair;
   }

   public int getJobThreads() {
      return this.jobThreads;
   }

   public Collection<String> getColumnFamilies() {
      return this.columnFamilies;
   }

   public Collection<Range<Token>> getRanges() {
      return this.ranges;
   }

   public Collection<String> getDataCenters() {
      return this.dataCenters;
   }

   public Collection<String> getHosts() {
      return this.hosts;
   }

   public PreviewKind getPreviewKind() {
      return this.previewKind;
   }

   public boolean isPreview() {
      return this.previewKind.isPreview();
   }

   public boolean isInLocalDCOnly() {
      return this.dataCenters.size() == 1 && this.dataCenters.contains(DatabaseDescriptor.getLocalDataCenter());
   }

   public String toString() {
      return "repair options (parallelism: " + this.parallelism + ", primary range: " + this.primaryRange + ", incremental: " + this.incremental + ", job threads: " + this.jobThreads + ", ColumnFamilies: " + this.columnFamilies + ", dataCenters: " + this.dataCenters + ", hosts: " + this.hosts + ", previewKind: " + this.previewKind + ", # of ranges: " + this.ranges.size() + ", pull repair: " + this.pullRepair + ", force repair: " + this.forceRepair + ')';
   }

   public Map<String, String> asMap() {
      Map<String, String> options = new HashMap();
      options.put("parallelism", this.parallelism.toString());
      options.put("primaryRange", Boolean.toString(this.primaryRange));
      options.put("incremental", Boolean.toString(this.incremental));
      options.put("jobThreads", Integer.toString(this.jobThreads));
      options.put("columnFamilies", Joiner.on(",").join(this.columnFamilies));
      options.put("dataCenters", Joiner.on(",").join(this.dataCenters));
      options.put("hosts", Joiner.on(",").join(this.hosts));
      options.put("trace", Boolean.toString(this.trace));
      options.put("ranges", Joiner.on(",").join(this.ranges));
      options.put("pullRepair", Boolean.toString(this.pullRepair));
      options.put("forceRepair", Boolean.toString(this.forceRepair));
      options.put("previewKind", this.previewKind.toString());
      return options;
   }
}
