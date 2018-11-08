package com.datastax.bdp.db.tools.nodesync;

import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.bdp.db.nodesync.UserValidationProposer;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.utils.Streams;
import org.apache.commons.lang3.StringUtils;

@Command(
   name = "cancel",
   description = "Cancel a user-triggered validation"
)
public class CancelValidation extends NodeSyncCommand {
   @Arguments(
      usage = "<id>",
      description = "The validation ID"
   )
   private String id = null;

   public CancelValidation() {
   }

   public final void execute(Metadata metadata, Session session, NodeProbes probes) {
      if(StringUtils.isBlank(this.id)) {
         throw new NodeSyncException("Validation ID is required");
      } else {
         Set<InetAddress> allLiveNodes = (Set)metadata.getAllHosts().stream().filter(Host::isUp).map(Host::getBroadcastAddress).collect(Collectors.toSet());
         Statement select = QueryBuilder.select(new String[]{"node", "status"}).from("system_distributed", "nodesync_user_validations").where(QueryBuilder.eq("id", this.id));
         Stream var10000 = Streams.of(session.execute(select)).filter((r) -> {
            return Objects.equals(r.getString("status"), UserValidationProposer.Status.RUNNING.toString());
         }).map((r) -> {
            return r.getInet("node");
         });
         allLiveNodes.getClass();
         Set<InetAddress> nodes = (Set)var10000.filter(allLiveNodes::contains).collect(Collectors.toSet());
         Set<InetAddress> successfulNodes = new HashSet(nodes.size());
         Set<InetAddress> failedNodes = new HashSet();
         Iterator var9 = nodes.iterator();

         while(var9.hasNext()) {
            InetAddress address = (InetAddress)var9.next();

            try {
               probes.cancelUserValidation(address, this.id);
               successfulNodes.add(address);
               this.printVerbose("%s: Cancelled", new Object[]{address});
            } catch (NodeSyncService.NotFoundValidationException var12) {
               successfulNodes.add(address);
               this.printVerbose("%s: Not found", new Object[]{address});
            } catch (NodeSyncService.CancelledValidationException var13) {
               successfulNodes.add(address);
               this.printVerbose("%s: Already cancelled", new Object[]{address});
            } catch (Exception var14) {
               failedNodes.add(address);
               System.err.printf("%s: Error while cancelling: %s%n", new Object[]{address, var14.getMessage()});
            }
         }

         if(!failedNodes.isEmpty()) {
            throw new NodeSyncException("The cancellation has failed in nodes: " + failedNodes);
         } else if(successfulNodes.isEmpty()) {
            throw new NodeSyncException("The validation to be cancelled hasn't been found in any node");
         } else {
            this.printVerbose("The validation has been cancelled in nodes %s", new Object[]{successfulNodes});
         }
      }
   }
}
