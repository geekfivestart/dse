package org.apache.cassandra.tools.nodetool;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.commons.lang3.StringUtils;

@Command(
   name = "repair",
   description = "Repair one or more tables"
)
public class Repair extends NodeTool.NodeToolCmd {
   public static final Set<String> ONLY_EXPLICITLY_REPAIRED = Sets.newHashSet(new String[]{"system_distributed"});
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "seqential",
      name = {"-seq", "--sequential"},
      description = "Use -seq to carry out a sequential repair"
   )
   private boolean sequential = false;
   @Option(
      title = "dc parallel",
      name = {"-dcpar", "--dc-parallel"},
      description = "Use -dcpar to repair data centers in parallel."
   )
   private boolean dcParallel = false;
   @Option(
      title = "local_dc",
      name = {"-local", "--in-local-dc"},
      description = "Use -local to only repair against nodes in the same datacenter"
   )
   private boolean localDC = false;
   @Option(
      title = "specific_dc",
      name = {"-dc", "--in-dc"},
      description = "Use -dc to repair specific datacenters"
   )
   private List<String> specificDataCenters = new ArrayList();
   @Option(
      title = "specific_host",
      name = {"-hosts", "--in-hosts"},
      description = "Use -hosts to repair specific hosts"
   )
   private List<String> specificHosts = new ArrayList();
   @Option(
      title = "start_token",
      name = {"-st", "--start-token"},
      description = "Use -st to specify a token at which the repair range starts"
   )
   private String startToken = "";
   @Option(
      title = "end_token",
      name = {"-et", "--end-token"},
      description = "Use -et to specify a token at which repair range ends"
   )
   private String endToken = "";
   @Option(
      title = "primary_range",
      name = {"-pr", "--partitioner-range"},
      description = "Use -pr to repair only the first range returned by the partitioner"
   )
   private boolean primaryRange = false;
   /** @deprecated */
   @Option(
      title = "full",
      name = {"-full", "--full"},
      description = "Use -full to issue a full repair. (default)"
   )
   @Deprecated
   private boolean fullOption = false;
   @Option(
      title = "incremental",
      name = {"-inc", "--inc"},
      description = "Use -inc to issue an incremental repair."
   )
   private boolean incrementalOption = false;
   @Option(
      title = "force",
      name = {"-force", "--force"},
      description = "Use -force to filter out down endpoints"
   )
   private boolean force = false;
   @Option(
      title = "preview",
      name = {"-prv", "--preview"},
      description = "Determine ranges and amount of data to be streamed, but don't actually perform repair"
   )
   private boolean preview = false;
   @Option(
      title = "validate",
      name = {"-vd", "--validate"},
      description = "Checks that repaired data is in sync between nodes. Out of sync repaired data indicates a full repair should be run."
   )
   private boolean validate = false;
   @Option(
      title = "job_threads",
      name = {"-j", "--job-threads"},
      description = "Number of threads to run repair jobs. Usually this means number of CFs to repair concurrently. WARNING: increasing this puts more load on repairing nodes, so be careful. (default: 1, max: 4)"
   )
   private int numJobThreads = 1;
   @Option(
      title = "trace_repair",
      name = {"-tr", "--trace"},
      description = "Use -tr to trace the repair. Traces are logged to system_traces.events."
   )
   private boolean trace = false;
   @Option(
      title = "pull_repair",
      name = {"-pl", "--pull"},
      description = "Use --pull to perform a one way repair where data is only streamed from a remote node to this node."
   )
   private boolean pullRepair = false;

   public Repair() {
   }

   private PreviewKind getPreviewKind() {
      return this.validate?PreviewKind.REPAIRED:(this.preview && !this.incrementalOption?PreviewKind.ALL:(this.preview?PreviewKind.UNREPAIRED:PreviewKind.NONE));
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe, NodeTool.NodeToolCmd.KeyspaceSet.NON_LOCAL_STRATEGY);
      String[] tableNames = this.parseOptionalTables(this.args);
      if(!this.primaryRange || this.specificDataCenters.isEmpty() && this.specificHosts.isEmpty()) {
         if(this.fullOption && this.incrementalOption) {
            throw new IllegalArgumentException("Cannot run both full and incremental repair, choose either --full or -inc option.");
         } else {
            Iterator var4 = keyspaces.iterator();

            while(true) {
               String keyspace;
               do {
                  if(!var4.hasNext()) {
                     return;
                  }

                  keyspace = (String)var4.next();
               } while((this.args == null || this.args.isEmpty()) && ONLY_EXPLICITLY_REPAIRED.contains(keyspace));

               Map<String, String> options = new HashMap();
               RepairParallelism parallelismDegree = RepairParallelism.PARALLEL;
               if(this.sequential) {
                  parallelismDegree = RepairParallelism.SEQUENTIAL;
               } else if(this.dcParallel) {
                  parallelismDegree = RepairParallelism.DATACENTER_AWARE;
               }

               options.put("parallelism", parallelismDegree.getName());
               options.put("primaryRange", Boolean.toString(this.primaryRange));
               options.put("incremental", Boolean.toString(this.incrementalOption));
               options.put("jobThreads", Integer.toString(this.numJobThreads));
               options.put("trace", Boolean.toString(this.trace));
               options.put("columnFamilies", StringUtils.join(tableNames, ","));
               options.put("pullRepair", Boolean.toString(this.pullRepair));
               options.put("forceRepair", Boolean.toString(this.force));
               options.put("previewKind", this.getPreviewKind().toString());
               if(!this.startToken.isEmpty() || !this.endToken.isEmpty()) {
                  options.put("ranges", this.startToken + ":" + this.endToken);
               }

               if(this.localDC) {
                  options.put("dataCenters", StringUtils.join(Lists.newArrayList(new String[]{probe.getDataCenter()}), ","));
               } else {
                  options.put("dataCenters", StringUtils.join(this.specificDataCenters, ","));
               }

               options.put("hosts", StringUtils.join(this.specificHosts, ","));

               try {
                  probe.repairAsync(System.out, keyspace, options);
               } catch (Exception var9) {
                  throw new RuntimeException("Error occurred during repair", var9);
               }
            }
         }
      } else {
         throw new RuntimeException("Primary range repair should be performed on all nodes in the cluster.");
      }
   }
}
