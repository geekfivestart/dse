package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCoordinator {
   private static final Logger logger = LoggerFactory.getLogger(StreamCoordinator.class);
   private static final DebuggableThreadPoolExecutor streamExecutor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("StreamConnectionEstablisher", FBUtilities.getAvailableProcessors());
   private final boolean connectSequentially;
   private Map<InetAddress, StreamCoordinator.HostStreamingData> peerSessions = new HashMap();
   private final int connectionsPerHost;
   private StreamConnectionFactory factory;
   private final boolean keepSSTableLevel;
   private Iterator<StreamSession> sessionsToConnect = null;
   private final UUID pendingRepair;
   private final PreviewKind previewKind;

   public StreamCoordinator(int connectionsPerHost, boolean keepSSTableLevel, StreamConnectionFactory factory, boolean connectSequentially, UUID pendingRepair, PreviewKind previewKind) {
      this.connectionsPerHost = connectionsPerHost;
      this.factory = factory;
      this.keepSSTableLevel = keepSSTableLevel;
      this.connectSequentially = connectSequentially;
      this.pendingRepair = pendingRepair;
      this.previewKind = previewKind;
   }

   public void setConnectionFactory(StreamConnectionFactory factory) {
      this.factory = factory;
   }

   public synchronized boolean hasActiveSessions() {
      Iterator var1 = this.peerSessions.values().iterator();

      StreamCoordinator.HostStreamingData data;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         data = (StreamCoordinator.HostStreamingData)var1.next();
      } while(!data.hasActiveSessions());

      return true;
   }

   public synchronized Collection<StreamSession> getAllStreamSessions() {
      Collection<StreamSession> results = new ArrayList();
      Iterator var2 = this.peerSessions.values().iterator();

      while(var2.hasNext()) {
         StreamCoordinator.HostStreamingData data = (StreamCoordinator.HostStreamingData)var2.next();
         results.addAll(data.getAllStreamSessions());
      }

      return results;
   }

   public boolean isReceiving() {
      return this.connectionsPerHost == 0;
   }

   public void connect(StreamResultFuture future) {
      if(this.connectSequentially) {
         this.connectSequentially(future);
      } else {
         this.connectAllStreamSessions();
      }

   }

   private void connectAllStreamSessions() {
      Iterator var1 = this.peerSessions.values().iterator();

      while(var1.hasNext()) {
         StreamCoordinator.HostStreamingData data = (StreamCoordinator.HostStreamingData)var1.next();
         data.connectAllStreamSessions();
      }

   }

   private void connectSequentially(StreamResultFuture future) {
      this.sessionsToConnect = this.getAllStreamSessions().iterator();
      future.addEventListener(new StreamEventHandler() {
         public void handleStreamEvent(StreamEvent event) {
            if(event.eventType == StreamEvent.Type.STREAM_PREPARED || event.eventType == StreamEvent.Type.STREAM_COMPLETE) {
               StreamCoordinator.this.connectNext();
            }

         }

         public void onSuccess(StreamState result) {
         }

         public void onFailure(Throwable t) {
         }
      });
      this.connectNext();
   }

   private void connectNext() {
      if(this.sessionsToConnect != null) {
         if(this.sessionsToConnect.hasNext()) {
            StreamSession next = (StreamSession)this.sessionsToConnect.next();
            logger.debug("Connecting next session {} with {}.", next.planId(), next.peer.getHostAddress());
            streamExecutor.execute(new StreamCoordinator.StreamSessionConnector(next));
         } else {
            logger.debug("Finished connecting all sessions");
         }

      }
   }

   public synchronized Set<InetAddress> getPeers() {
      return new HashSet(this.peerSessions.keySet());
   }

   public synchronized StreamSession getOrCreateNextSession(InetAddress peer, InetAddress connecting) {
      return this.getOrCreateHostData(peer).getOrCreateNextSession(peer, connecting);
   }

   public synchronized StreamSession getOrCreateSessionById(InetAddress peer, int id, InetAddress connecting) {
      return this.getOrCreateHostData(peer).getOrCreateSessionById(peer, id, connecting);
   }

   public synchronized void updateProgress(ProgressInfo info) {
      this.getHostData(info.peer).updateProgress(info);
   }

   public synchronized void addSessionInfo(SessionInfo session) {
      StreamCoordinator.HostStreamingData data = this.getOrCreateHostData(session.peer);
      data.addSessionInfo(session);
   }

   public synchronized Set<SessionInfo> getAllSessionInfo() {
      Set<SessionInfo> result = new HashSet();
      Iterator var2 = this.peerSessions.values().iterator();

      while(var2.hasNext()) {
         StreamCoordinator.HostStreamingData data = (StreamCoordinator.HostStreamingData)var2.next();
         result.addAll(data.getAllSessionInfo());
      }

      return result;
   }

   public synchronized void transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails) {
      StreamCoordinator.HostStreamingData sessionList = this.getOrCreateHostData(to);
      if(this.connectionsPerHost > 1) {
         List<List<StreamSession.SSTableStreamingSections>> buckets = this.sliceSSTableDetails(sstableDetails);
         Iterator var5 = buckets.iterator();

         while(var5.hasNext()) {
            List<StreamSession.SSTableStreamingSections> subList = (List)var5.next();
            StreamSession session = sessionList.getOrCreateNextSession(to, to);
            session.addTransferFiles(subList);
         }
      } else {
         StreamSession session = sessionList.getOrCreateNextSession(to, to);
         session.addTransferFiles(sstableDetails);
      }

   }

   private List<List<StreamSession.SSTableStreamingSections>> sliceSSTableDetails(Collection<StreamSession.SSTableStreamingSections> sstableDetails) {
      int targetSlices = Math.min(sstableDetails.size(), this.connectionsPerHost);
      int step = Math.round((float)sstableDetails.size() / (float)targetSlices);
      int index = 0;
      List<List<StreamSession.SSTableStreamingSections>> result = new ArrayList();
      List<StreamSession.SSTableStreamingSections> slice = null;
      Iterator iter = sstableDetails.iterator();

      while(iter.hasNext()) {
         StreamSession.SSTableStreamingSections streamSession = (StreamSession.SSTableStreamingSections)iter.next();
         if(index % step == 0) {
            slice = new ArrayList();
            result.add(slice);
         }

         slice.add(streamSession);
         ++index;
         iter.remove();
      }

      return result;
   }

   private StreamCoordinator.HostStreamingData getHostData(InetAddress peer) {
      StreamCoordinator.HostStreamingData data = (StreamCoordinator.HostStreamingData)this.peerSessions.get(peer);
      if(data == null) {
         throw new IllegalArgumentException("Unknown peer requested: " + peer);
      } else {
         return data;
      }
   }

   private StreamCoordinator.HostStreamingData getOrCreateHostData(InetAddress peer) {
      StreamCoordinator.HostStreamingData data = (StreamCoordinator.HostStreamingData)this.peerSessions.get(peer);
      if(data == null) {
         data = new StreamCoordinator.HostStreamingData(null);
         this.peerSessions.put(peer, data);
      }

      return data;
   }

   public UUID getPendingRepair() {
      return this.pendingRepair;
   }

   private class HostStreamingData {
      private Map<Integer, StreamSession> streamSessions;
      private Map<Integer, SessionInfo> sessionInfos;
      private int lastReturned;

      private HostStreamingData() {
         this.streamSessions = new HashMap();
         this.sessionInfos = new HashMap();
         this.lastReturned = -1;
      }

      public boolean hasActiveSessions() {
         Iterator var1 = this.streamSessions.values().iterator();

         StreamSession.State state;
         do {
            if(!var1.hasNext()) {
               return false;
            }

            StreamSession session = (StreamSession)var1.next();
            state = session.state();
         } while(state.finalState);

         return true;
      }

      public StreamSession getOrCreateNextSession(InetAddress peer, InetAddress connecting) {
         if(this.streamSessions.size() < StreamCoordinator.this.connectionsPerHost) {
            StreamSession session = new StreamSession(peer, connecting, StreamCoordinator.this.factory, this.streamSessions.size(), StreamCoordinator.this.keepSSTableLevel, StreamCoordinator.this.pendingRepair, StreamCoordinator.this.previewKind);
            this.streamSessions.put(Integer.valueOf(++this.lastReturned), session);
            return session;
         } else {
            if(this.lastReturned >= this.streamSessions.size() - 1) {
               this.lastReturned = 0;
            }

            return (StreamSession)this.streamSessions.get(Integer.valueOf(this.lastReturned++));
         }
      }

      public void connectAllStreamSessions() {
         Iterator var1 = this.streamSessions.values().iterator();

         while(var1.hasNext()) {
            StreamSession session = (StreamSession)var1.next();
            StreamCoordinator.streamExecutor.execute(new StreamCoordinator.StreamSessionConnector(session));
         }

      }

      public Collection<StreamSession> getAllStreamSessions() {
         return Collections.unmodifiableCollection(this.streamSessions.values());
      }

      public StreamSession getOrCreateSessionById(InetAddress peer, int id, InetAddress connecting) {
         StreamSession session = (StreamSession)this.streamSessions.get(Integer.valueOf(id));
         if(session == null) {
            session = new StreamSession(peer, connecting, StreamCoordinator.this.factory, id, StreamCoordinator.this.keepSSTableLevel, StreamCoordinator.this.pendingRepair, StreamCoordinator.this.previewKind);
            this.streamSessions.put(Integer.valueOf(id), session);
         }

         return session;
      }

      public void updateProgress(ProgressInfo info) {
         ((SessionInfo)this.sessionInfos.get(Integer.valueOf(info.sessionIndex))).updateProgress(info);
      }

      public void addSessionInfo(SessionInfo info) {
         this.sessionInfos.merge(Integer.valueOf(info.sessionIndex), info, (previous, value) -> {
            value.copyProgress(previous);
            return value;
         });
      }

      public Collection<SessionInfo> getAllSessionInfo() {
         return this.sessionInfos.values();
      }
   }

   private static class StreamSessionConnector implements Runnable {
      private final StreamSession session;

      public StreamSessionConnector(StreamSession session) {
         this.session = session;
      }

      public void run() {
         this.session.start();
         StreamCoordinator.logger.info("[Stream #{}, ID#{}] Beginning stream session with {}", new Object[]{this.session.planId(), Integer.valueOf(this.session.sessionIndex()), this.session.peer});
      }
   }
}
