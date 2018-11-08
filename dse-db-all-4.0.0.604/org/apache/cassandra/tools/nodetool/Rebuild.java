package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "rebuild",
   description = "Rebuild data by streaming from other nodes (similarly to bootstrap)"
)
public class Rebuild extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<src-dc-name>",
      description = "Name of DC from which to select sources for streaming. By default, pick any DC. Must not be used in combination with --dcs."
   )
   private String sourceDataCenterName = null;
   @Option(
      title = "specific_keyspace",
      name = {"-ks", "--keyspace"},
      description = "Use -ks to rebuild specific one or more keyspaces (comma separated list)."
   )
   private String keyspace = null;
   @Option(
      title = "specific_tokens",
      name = {"-ts", "--tokens"},
      description = "Use -ts to rebuild specific token ranges, in the format of \"(start_token_1,end_token_1],(start_token_2,end_token_2],...(start_token_n,end_token_n]\"."
   )
   private String tokens = null;
   @Option(
      title = "mode",
      allowedValues = {"normal", "refetch", "reset", "reset-no-shapshot"},
      name = {"-m", "--mode"},
      description = "normal: conventional behaviour, only streams ranges that are not already locally available (this is the default) - refetch: resets the locally available ranges, streams all ranges but leaves current data untouched - reset: resets the locally available ranges, removes all locally present data (like a TRUNCATE), streams all ranges - reset-no-snapshot: like 'reset', but prevents a snapshot if 'auto_snapshot' is enabled."
   )
   private String mode = "normal";
   @Option(
      title = "connections_per_host",
      name = {"-c", "--connections-per-host"},
      description = "Use -c to override the value configured for streaming_connections_per_host in cassandra.yaml."
   )
   private int streamingConnectionsPerHost = 0;
   @Option(
      title = "specific_sources",
      name = {"-s", "--sources"},
      description = "Use -s to specify hosts that this node should stream. Multiple hosts should be separated using commas (e.g. 127.0.0.1,127.0.0.2,...)"
   )
   private String specificSources = null;
   @Option(
      title = "exclude_sources",
      name = {"-x", "--exclude-sources"},
      description = "Use -x to specify hosts that this node should not stream from. Multiple hosts should be separated using commas (e.g. 127.0.0.1,127.0.0.2,...)"
   )
   private String excludeSources = null;
   @Option(
      title = "src_dc_names",
      name = {"-dc", "--dcs"},
      description = "List of DC names or DC+Rack to stream from. Multiple DCs should be separated using commas (e.g. dc-a,dc-b,...). To also incude a rack name, separate DC and rack name by a colon ':' (e.g. dc-a:rack1,dc-a:rack2)."
   )
   private String sourceDCs = null;
   @Option(
      title = "exclude_dc_names",
      name = {"-xdc", "--exclude-dcs"},
      description = "List of DC names or DC+Rack to exclude from streaming. Multiple DCs should be separated using commas (e.g. dc-a,dc-b,...). To also incude a rack name, separate DC and rack name by a colon ':' (e.g. dc-a:rack1,dc-a:rack2)."
   )
   private String excludeDCs = null;

   public Rebuild() {
   }

   public void execute(NodeProbe probe) {
      if(this.sourceDataCenterName != null && this.sourceDCs != null) {
         throw new IllegalArgumentException("Use either the name of the source DC (command argument) or a list of source DCs (-dc option) to include but not both");
      } else {
         List<String> keyspaces = commaSeparatedList(this.keyspace);
         List<String> whitelistDCs = this.sourceDataCenterName != null?Collections.singletonList(this.sourceDataCenterName):commaSeparatedList(this.sourceDCs);
         List<String> blacklistDcs = commaSeparatedList(this.excludeDCs);
         List<String> whitelistSources = commaSeparatedList(this.specificSources);
         List<String> blacklistSources = commaSeparatedList(this.excludeSources);
         String msg = probe.rebuild(keyspaces, this.tokens, this.mode, this.streamingConnectionsPerHost, whitelistDCs, blacklistDcs, whitelistSources, blacklistSources);
         System.out.println(msg);
      }
   }

   private static List<String> commaSeparatedList(String s) {
      return s == null?null:(List)Arrays.stream(s.split(",")).map(String::trim).collect(Collectors.toList());
   }
}
