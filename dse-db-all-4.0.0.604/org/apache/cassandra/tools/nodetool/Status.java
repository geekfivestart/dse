package org.apache.cassandra.tools.nodetool;

import com.google.common.collect.ArrayListMultimap;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Map.Entry;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "status",
   description = "Print cluster information (state, load, IDs, ...)"
)
public class Status extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace>]",
      description = "The keyspace name"
   )
   private String keyspace = null;
   @Option(
      title = "resolve_ip",
      name = {"-r", "--resolve-ip"},
      description = "Show node domain names instead of IPs"
   )
   private boolean resolveIp = false;
   private boolean isTokenPerNode = true;
   private int maxAddressLength = 0;
   private String format = null;
   private Collection<String> joiningNodes;
   private Collection<String> leavingNodes;
   private Collection<String> movingNodes;
   private Collection<String> liveNodes;
   private Collection<String> unreachableNodes;
   private Map<String, String> loadMap;
   private Map<String, String> hostIDMap;
   private EndpointSnitchInfoMBean epSnitchInfo;

   public Status() {
   }

   public void execute(NodeProbe probe) {
      this.joiningNodes = probe.getJoiningNodes();
      this.leavingNodes = probe.getLeavingNodes();
      this.movingNodes = probe.getMovingNodes();
      this.loadMap = probe.getLoadMap();
      Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
      this.liveNodes = probe.getLiveNodes();
      this.unreachableNodes = probe.getUnreachableNodes();
      this.hostIDMap = probe.getHostIdMap();
      this.epSnitchInfo = probe.getEndpointSnitchInfoProxy();
      StringBuilder errors = new StringBuilder();
      Map<InetAddress, Float> ownerships = null;
      boolean hasEffectiveOwns = false;

      try {
         ownerships = probe.effectiveOwnership(this.keyspace);
         hasEffectiveOwns = true;
      } catch (IllegalStateException var15) {
         ownerships = probe.getOwnership();
         errors.append("Note: ").append(var15.getMessage()).append("%n");
      } catch (IllegalArgumentException var16) {
         System.out.printf("%nError: %s%n", new Object[]{var16.getMessage()});
         System.exit(1);
      }

      SortedMap<String, SetHostStat> dcs = NodeTool.getOwnershipByDc(probe, this.resolveIp, tokensToEndpoints, ownerships);
      if(dcs.values().size() < tokensToEndpoints.keySet().size()) {
         this.isTokenPerNode = false;
      }

      this.findMaxAddressLength(dcs);
      Iterator var7 = dcs.entrySet().iterator();

      while(var7.hasNext()) {
         Entry<String, SetHostStat> dc = (Entry)var7.next();
         String dcHeader = String.format("Datacenter: %s%n", new Object[]{dc.getKey()});
         System.out.printf(dcHeader, new Object[0]);

         for(int i = 0; i < dcHeader.length() - 1; ++i) {
            System.out.print('=');
         }

         System.out.println();
         System.out.println("Status=Up/Down");
         System.out.println("|/ State=Normal/Leaving/Joining/Moving");
         this.printNodesHeader(hasEffectiveOwns, this.isTokenPerNode);
         ArrayListMultimap<InetAddress, HostStat> hostToTokens = ArrayListMultimap.create();
         Iterator var11 = ((SetHostStat)dc.getValue()).iterator();

         while(var11.hasNext()) {
            HostStat stat = (HostStat)var11.next();
            hostToTokens.put(stat.endpoint, stat);
         }

         var11 = hostToTokens.keySet().iterator();

         while(var11.hasNext()) {
            InetAddress endpoint = (InetAddress)var11.next();
            Float owns = (Float)ownerships.get(endpoint);
            List<HostStat> tokens = hostToTokens.get(endpoint);
            this.printNode(endpoint.getHostAddress(), owns, tokens, hasEffectiveOwns, this.isTokenPerNode);
         }
      }

      System.out.printf("%n" + errors.toString(), new Object[0]);
   }

   private void findMaxAddressLength(Map<String, SetHostStat> dcs) {
      this.maxAddressLength = 0;
      Iterator var2 = dcs.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<String, SetHostStat> dc = (Entry)var2.next();

         HostStat stat;
         for(Iterator var4 = ((SetHostStat)dc.getValue()).iterator(); var4.hasNext(); this.maxAddressLength = Math.max(this.maxAddressLength, stat.ipOrDns().length())) {
            stat = (HostStat)var4.next();
         }
      }

   }

   private void printNodesHeader(boolean hasEffectiveOwns, boolean isTokenPerNode) {
      String fmt = this.getFormat(hasEffectiveOwns, isTokenPerNode);
      String owns = hasEffectiveOwns?"Owns (effective)":"Owns";
      if(isTokenPerNode) {
         System.out.printf(fmt, new Object[]{"-", "-", "Address", "Load", owns, "Host ID", "Token", "Rack"});
      } else {
         System.out.printf(fmt, new Object[]{"-", "-", "Address", "Load", "Tokens", owns, "Host ID", "Rack"});
      }

   }

   private void printNode(String endpoint, Float owns, List<HostStat> tokens, boolean hasEffectiveOwns, boolean isTokenPerNode) {
      String fmt = this.getFormat(hasEffectiveOwns, isTokenPerNode);
      String status;
      if(this.liveNodes.contains(endpoint)) {
         status = "U";
      } else if(this.unreachableNodes.contains(endpoint)) {
         status = "D";
      } else {
         status = "?";
      }

      String state;
      if(this.joiningNodes.contains(endpoint)) {
         state = "J";
      } else if(this.leavingNodes.contains(endpoint)) {
         state = "L";
      } else if(this.movingNodes.contains(endpoint)) {
         state = "M";
      } else {
         state = "N";
      }

      String load = this.loadMap.containsKey(endpoint)?(String)this.loadMap.get(endpoint):"?";
      String strOwns = owns != null && hasEffectiveOwns?(new DecimalFormat("##0.0%")).format(owns):"?";
      String hostID = (String)this.hostIDMap.get(endpoint);

      String rack;
      try {
         rack = this.epSnitchInfo.getRack(endpoint);
      } catch (UnknownHostException var14) {
         throw new RuntimeException(var14);
      }

      String endpointDns = ((HostStat)tokens.get(0)).ipOrDns();
      if(isTokenPerNode) {
         System.out.printf(fmt, new Object[]{status, state, endpointDns, load, strOwns, hostID, ((HostStat)tokens.get(0)).token, rack});
      } else {
         System.out.printf(fmt, new Object[]{status, state, endpointDns, load, Integer.valueOf(tokens.size()), strOwns, hostID, rack});
      }

   }

   private String getFormat(boolean hasEffectiveOwns, boolean isTokenPerNode) {
      if(this.format == null) {
         StringBuilder buf = new StringBuilder();
         String addressPlaceholder = String.format("%%-%ds  ", new Object[]{Integer.valueOf(this.maxAddressLength)});
         buf.append("%s%s  ");
         buf.append(addressPlaceholder);
         buf.append("%-9s  ");
         if(!isTokenPerNode) {
            buf.append("%-11s  ");
         }

         if(hasEffectiveOwns) {
            buf.append("%-16s  ");
         } else {
            buf.append("%-6s  ");
         }

         buf.append("%-36s  ");
         if(isTokenPerNode) {
            buf.append("%-39s  ");
         }

         buf.append("%s%n");
         this.format = buf.toString();
      }

      return this.format;
   }
}
