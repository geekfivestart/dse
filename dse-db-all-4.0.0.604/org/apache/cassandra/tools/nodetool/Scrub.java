package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "scrub",
   description = "Scrub (rebuild sstables for) one or more tables"
)
public class Scrub extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "disable_snapshot",
      name = {"-ns", "--no-snapshot"},
      description = "Scrubbed CFs will be snapshotted first, if disableSnapshot is false. (default false)"
   )
   private boolean disableSnapshot = false;
   @Option(
      title = "skip_corrupted",
      name = {"-s", "--skip-corrupted"},
      description = "Skip corrupted partitions even when scrubbing counter tables. (default false)"
   )
   private boolean skipCorrupted = false;
   @Option(
      title = "no_validate",
      name = {"-n", "--no-validate"},
      description = "Do not validate columns using column validator"
   )
   private boolean noValidation = false;
   @Option(
      title = "reinsert_overflowed_ttl",
      name = {"-r", "--reinsert-overflowed-ttl"},
      description = "Rewrites rows with overflowed expiration date affected by CASSANDRA-14092 with the maximum supported expiration date of 2038-01-19T03:14:06+00:00. The rows are rewritten with the original timestamp incremented by one millisecond to override/supersede any potential tombstone that may have been generated during compaction of the affected rows."
   )
   private boolean reinsertOverflowedTTL = false;
   @Option(
      title = "jobs",
      name = {"-j", "--jobs"},
      description = "Number of sstables to scrub simultanously, set to 0 to use all available compaction threads"
   )
   private int jobs = 2;

   public Scrub() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
      String[] tableNames = this.parseOptionalTables(this.args);
      Iterator var4 = keyspaces.iterator();

      while(var4.hasNext()) {
         String keyspace = (String)var4.next();

         try {
            probe.scrub(System.out, this.disableSnapshot, this.skipCorrupted, !this.noValidation, this.reinsertOverflowedTTL, this.jobs, keyspace, tableNames);
         } catch (IllegalArgumentException var7) {
            throw var7;
         } catch (Exception var8) {
            throw new RuntimeException("Error occurred during scrubbing", var8);
         }
      }

   }
}
