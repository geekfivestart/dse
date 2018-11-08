package org.apache.cassandra.tools.nodetool;

import com.google.common.collect.LinkedHashMultimap;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "ring",
   description = "Print information about the token ring"
)
public class Ring extends NodeTool.NodeToolCmd {
   @Arguments(
      description = "Specify a keyspace for accurate ownership information (topology awareness)"
   )
   private String keyspace = null;
   @Option(
      title = "resolve_ip",
      name = {"-r", "--resolve-ip"},
      description = "Show node domain names instead of IPs"
   )
   private boolean resolveIp = false;

   public Ring() {
   }

   public void execute(NodeProbe probe) {
      Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
      LinkedHashMultimap<String, String> endpointsToTokens = LinkedHashMultimap.create();
      boolean haveVnodes = false;
      Iterator var5 = tokensToEndpoints.entrySet().iterator();

      while(var5.hasNext()) {
         Entry<String, String> entry = (Entry)var5.next();
         haveVnodes |= endpointsToTokens.containsKey(entry.getValue());
         endpointsToTokens.put(entry.getValue(), entry.getKey());
      }

      int maxAddressLength = ((String)Collections.max(endpointsToTokens.keys(), new Comparator<String>() {
         public int compare(String first, String second) {
            return Integer.compare(first.length(), second.length());
         }
      })).length();
      String formatPlaceholder = "%%-%ds  %%-12s%%-7s%%-8s%%-16s%%-20s%%-44s%%n";
      String format = String.format(formatPlaceholder, new Object[]{Integer.valueOf(maxAddressLength)});
      StringBuilder errors = new StringBuilder();
      boolean showEffectiveOwnership = true;

      Map ownerships;
      try {
         ownerships = probe.effectiveOwnership(this.keyspace);
      } catch (IllegalStateException var13) {
         ownerships = probe.getOwnership();
         errors.append("Note: ").append(var13.getMessage()).append("%n");
         showEffectiveOwnership = false;
      } catch (IllegalArgumentException var14) {
         System.out.printf("%nError: %s%n", new Object[]{var14.getMessage()});
         return;
      }

      System.out.println();
      Iterator var11 = NodeTool.getOwnershipByDc(probe, this.resolveIp, tokensToEndpoints, ownerships).entrySet().iterator();

      while(var11.hasNext()) {
         Entry<String, SetHostStat> entry = (Entry)var11.next();
         this.printDc(probe, format, (String)entry.getKey(), endpointsToTokens, (SetHostStat)entry.getValue(), showEffectiveOwnership);
      }

      if(haveVnodes) {
         System.out.println("  Warning: \"nodetool ring\" is used to output all the tokens of a node.");
         System.out.println("  To view status related info of a node use \"nodetool status\" instead.\n");
      }

      System.out.printf("%n  " + errors.toString(), new Object[0]);
   }

   private void printDc(NodeProbe probe, String format, String dc, LinkedHashMultimap<String, String> endpointsToTokens, SetHostStat hoststats, boolean showEffectiveOwnership) {
      Collection<String> liveNodes = probe.getLiveNodes();
      Collection<String> deadNodes = probe.getUnreachableNodes();
      Collection<String> joiningNodes = probe.getJoiningNodes();
      Collection<String> leavingNodes = probe.getLeavingNodes();
      Collection<String> movingNodes = probe.getMovingNodes();
      Map<String, String> loadMap = probe.getLoadMap();
      System.out.println("Datacenter: " + dc);
      System.out.println("==========");
      List<String> tokens = new ArrayList();
      String lastToken = "";

      Iterator var15;
      HostStat stat;
      for(var15 = hoststats.iterator(); var15.hasNext(); lastToken = (String)tokens.get(tokens.size() - 1)) {
         stat = (HostStat)var15.next();
         tokens.addAll(endpointsToTokens.get(stat.endpoint.getHostAddress()));
      }

      System.out.printf(format, new Object[]{"Address", "Rack", "Status", "State", "Load", "Owns", "Token"});
      if(hoststats.size() > 1) {
         System.out.printf(format, new Object[]{"", "", "", "", "", "", lastToken});
      } else {
         System.out.println();
      }

      var15 = hoststats.iterator();

      while(var15.hasNext()) {
         stat = (HostStat)var15.next();
         String endpoint = stat.endpoint.getHostAddress();

         String rack;
         try {
            rack = probe.getEndpointSnitchInfoProxy().getRack(endpoint);
         } catch (UnknownHostException var23) {
            rack = "Unknown";
         }

         String status = liveNodes.contains(endpoint)?"Up":(deadNodes.contains(endpoint)?"Down":"?");
         String state = "Normal";
         if(joiningNodes.contains(endpoint)) {
            state = "Joining";
         } else if(leavingNodes.contains(endpoint)) {
            state = "Leaving";
         } else if(movingNodes.contains(endpoint)) {
            state = "Moving";
         }

         String load = loadMap.containsKey(endpoint)?(String)loadMap.get(endpoint):"?";
         String owns = stat.owns != null && showEffectiveOwnership?(new DecimalFormat("##0.00%")).format(stat.owns):"?";
         System.out.printf(format, new Object[]{stat.ipOrDns(), rack, status, state, load, owns, stat.token});
      }

      System.out.println();
   }
}
