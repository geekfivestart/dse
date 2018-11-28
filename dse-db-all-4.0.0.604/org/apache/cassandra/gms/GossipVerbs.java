package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.DroppingResponseException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipVerbs extends VerbGroup<GossipVerbs.GossipVersion> {
   private static final Logger logger = LoggerFactory.getLogger(GossipVerbs.class);
   public final Verb.OneWay<GossipDigestSyn> SYN;
   public final Verb.OneWay<GossipDigestAck> ACK;
   public final Verb.OneWay<GossipDigestAck2> ACK2;
   public final Verb.OneWay<EmptyPayload> SHUTDOWN;
   public final Verb.AckedRequest<EmptyPayload> ECHO;

   public GossipVerbs(Verbs.Group id) {
      super(id, true, GossipVerbs.GossipVersion.class);
      VerbGroup<GossipVerbs.GossipVersion>.RegistrationHelper helper = this.helper().stage(Stage.GOSSIP).droppedGroup(DroppedMessages.Group.OTHER);
      this.SYN = helper.oneWay("SYN", GossipDigestSyn.class).handler((VerbHandlers.OneWay)(new GossipVerbs.SynHandler()));
      this.ACK = helper.oneWay("ACK", GossipDigestAck.class).handler((VerbHandlers.OneWay)(new GossipVerbs.AckHandler()));
      this.ACK2 = helper.oneWay("ACK2", GossipDigestAck2.class).handler((VerbHandlers.OneWay)(new GossipVerbs.Ack2Handler()));
      this.SHUTDOWN = helper.oneWay("SHUTDOWN", EmptyPayload.class).handler((VerbHandlers.OneWay)(new GossipVerbs.ShutdownHandler()));
      this.ECHO = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("ECHO", EmptyPayload.class).timeout(DatabaseDescriptor::getRpcTimeout)).syncHandler(new GossipVerbs.EchoHandler());
   }

   private static class EchoHandler implements VerbHandlers.SyncAckedRequest<EmptyPayload> {
      private EchoHandler() {
      }

      public void handleSync(InetAddress from, EmptyPayload message) {
         if(!StorageService.instance.isSafeToReplyEchos()) {
            throw new DroppingResponseException();
         }
      }
   }

   private static class ShutdownHandler implements VerbHandlers.OneWay<EmptyPayload> {
      private ShutdownHandler() {
      }

      public void handle(InetAddress from, EmptyPayload payload) {
         if(!Gossiper.instance.isEnabled()) {
            GossipVerbs.logger.debug("Ignoring shutdown message from {} because gossip is disabled", from);
         } else {
            Gossiper.instance.markAsShutdown(from);
         }

      }
   }

   private static class Ack2Handler implements VerbHandlers.OneWay<GossipDigestAck2> {
      private Ack2Handler() {
      }

      public void handle(InetAddress from, GossipDigestAck2 message) {
         if(!Gossiper.instance.isEnabled()) {
            GossipVerbs.logger.trace("Ignoring GossipDigestAck2Message because gossip is disabled");
         } else {
            Map<InetAddress, EndpointState> remoteEpStateMap = message.getEndpointStateMap();
            Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
            Gossiper.instance.applyStateLocally(remoteEpStateMap);
            Gossiper.instance.onNewMessageProcessed();
         }
      }
   }

   private static class AckHandler implements VerbHandlers.OneWay<GossipDigestAck> {
      private AckHandler() {
      }

      public void handle(InetAddress from, GossipDigestAck message) {
         if(!Gossiper.instance.isEnabled() && !Gossiper.instance.isInShadowRound()) {
            GossipVerbs.logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
         } else {
            List<GossipDigest> gDigestList = message.getGossipDigestList();
            Map<InetAddress, EndpointState> epStateMap = message.getEndpointStateMap();
            GossipVerbs.logger.trace("Received ack with {} digests and {} states", Integer.valueOf(gDigestList.size()), Integer.valueOf(epStateMap.size()));
            if(Gossiper.instance.isInShadowRound()) {
               if(GossipVerbs.logger.isDebugEnabled()) {
                  GossipVerbs.logger.debug("Received an ack from {}, which may trigger exit from shadow round", from);
               }

               Gossiper.instance.maybeFinishShadowRound(from, gDigestList.isEmpty() && epStateMap.isEmpty(), epStateMap);
            } else {
               if(epStateMap.size() > 0) {
                  if(System.nanoTime() - Gossiper.instance.firstSynSendAt < 0L || Gossiper.instance.firstSynSendAt == 0L) {
                     if(GossipVerbs.logger.isTraceEnabled()) {
                        GossipVerbs.logger.trace("Ignoring unrequested GossipDigestAck from {}", from);
                     }

                     return;
                  }

                  Gossiper.instance.notifyFailureDetector(epStateMap);
                  Gossiper.instance.applyStateLocally(epStateMap);
               }

               Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap();
               Iterator var6 = gDigestList.iterator();

               while(var6.hasNext()) {
                  GossipDigest gDigest = (GossipDigest)var6.next();
                  InetAddress addr = gDigest.getEndpoint();
                  EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
                  if(localEpStatePtr != null) {
                     deltaEpStateMap.put(addr, localEpStatePtr);
                  }
               }

               if(GossipVerbs.logger.isTraceEnabled()) {
                  GossipVerbs.logger.trace("Sending a GossipDigestAck2Message to {}", from);
               }

               MessagingService.instance().send(Verbs.GOSSIP.ACK2.newRequest(from, (new GossipDigestAck2(deltaEpStateMap))));
               Gossiper.instance.onNewMessageProcessed();
            }
         }
      }
   }

   private static class SynHandler implements VerbHandlers.OneWay<GossipDigestSyn> {
      private SynHandler() {
      }

      public void handle(InetAddress from, GossipDigestSyn message) {
         if(!Gossiper.instance.isEnabled() && !Gossiper.instance.isInShadowRound()) {
            GossipVerbs.logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
         } else if(!message.clusterId.equals(DatabaseDescriptor.getClusterName())) {
            GossipVerbs.logger.warn("ClusterName mismatch from {} {}!={}", new Object[]{from, message.clusterId, DatabaseDescriptor.getClusterName()});
         } else if(message.partioner != null && !message.partioner.equals(DatabaseDescriptor.getPartitionerName())) {
            GossipVerbs.logger.warn("Partitioner mismatch from {} {}!={}", new Object[]{from, message.partioner, DatabaseDescriptor.getPartitionerName()});
         } else {
            List<GossipDigest> gDigestList = message.getGossipDigests();
            if(!Gossiper.instance.isEnabled() && Gossiper.instance.isInShadowRound()) {
               if(gDigestList.size() > 0) {
                  GossipVerbs.logger.debug("Ignoring non-empty GossipDigestSynMessage because currently in gossip shadow round");
               } else {
                  GossipVerbs.logger.debug("Received a shadow round syn from {}. Gossip is disabled but currently also in shadow round, responding with a minimal ack", from);
                  this.ack(from, new ArrayList(), new HashMap());
               }
            } else {
               if(GossipVerbs.logger.isTraceEnabled()) {
                  StringBuilder sb = new StringBuilder();
                  Iterator var5 = gDigestList.iterator();

                  while(var5.hasNext()) {
                     GossipDigest gDigest = (GossipDigest)var5.next();
                     sb.append(gDigest);
                     sb.append(' ');
                  }

                  GossipVerbs.logger.trace("Gossip syn digests are : {}", sb);
               }

               this.doSort(gDigestList);
               List<GossipDigest> deltaGossipDigestList = new ArrayList();
               Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap();
               Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);
               if(GossipVerbs.logger.isTraceEnabled()) {
                  GossipVerbs.logger.trace("sending {} digests and {} deltas", Integer.valueOf(deltaGossipDigestList.size()), Integer.valueOf(deltaEpStateMap.size()));
                  GossipVerbs.logger.trace("Sending a GossipDigestAckMessage to {}", from);
               }

               this.ack(from, deltaGossipDigestList, deltaEpStateMap);
            }
         }
      }

      private void ack(InetAddress to, List<GossipDigest> digestList, Map<InetAddress, EndpointState> epStateMap) {
         MessagingService.instance().send(Verbs.GOSSIP.ACK.newRequest(to, (new GossipDigestAck(digestList, epStateMap))));
         Gossiper.instance.onNewMessageProcessed();
      }

      private void doSort(List<GossipDigest> gDigestList) {
         Map<InetAddress, GossipDigest> epToDigestMap = new HashMap();
         Iterator var3 = gDigestList.iterator();

         while(var3.hasNext()) {
            GossipDigest gDigest = (GossipDigest)var3.next();
            epToDigestMap.put(gDigest.getEndpoint(), gDigest);
         }

         List<GossipDigest> diffDigests = new ArrayList(gDigestList.size());
         Iterator var11 = gDigestList.iterator();

         while(var11.hasNext()) {
            GossipDigest gDigest = (GossipDigest)var11.next();
            InetAddress ep = gDigest.getEndpoint();
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
            int version = epState != null?Gossiper.instance.getMaxEndpointStateVersion(epState):0;
            int diffVersion = Math.abs(version - gDigest.getMaxVersion());
            diffDigests.add(new GossipDigest(ep, gDigest.getGeneration(), diffVersion));
         }

         gDigestList.clear();
         Collections.sort(diffDigests);
         int size = diffDigests.size();

         for(int i = size - 1; i >= 0; --i) {
            gDigestList.add(epToDigestMap.get(((GossipDigest)diffDigests.get(i)).getEndpoint()));
         }

      }
   }

   public static enum GossipVersion implements Version<GossipVerbs.GossipVersion> {
      OSS_30;

      private GossipVersion() {
      }
   }
}
