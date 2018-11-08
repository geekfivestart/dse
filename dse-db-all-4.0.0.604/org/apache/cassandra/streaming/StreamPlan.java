package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.UUIDGen;

public class StreamPlan {
   public static final String[] EMPTY_COLUMN_FAMILIES = new String[0];
   private final UUID planId;
   private final StreamOperation streamOperation;
   private final List<StreamEventHandler> handlers;
   private final StreamCoordinator coordinator;
   private boolean flushBeforeTransfer;

   public StreamPlan(StreamOperation streamOperation) {
      this(streamOperation, 1, false, false, ActiveRepairService.NO_PENDING_REPAIR, PreviewKind.NONE);
   }

   public StreamPlan(StreamOperation streamOperation, boolean keepSSTableLevels, boolean connectSequentially) {
      this(streamOperation, 1, keepSSTableLevels, connectSequentially, ActiveRepairService.NO_PENDING_REPAIR, PreviewKind.NONE);
   }

   public StreamPlan(StreamOperation streamOperation, int connectionsPerHost, boolean keepSSTableLevels, boolean connectSequentially, UUID pendingRepair, PreviewKind previewKind) {
      this.planId = UUIDGen.getTimeUUID();
      this.handlers = new ArrayList();
      this.flushBeforeTransfer = true;
      this.streamOperation = streamOperation;
      this.coordinator = new StreamCoordinator(connectionsPerHost, keepSSTableLevels, new DefaultConnectionFactory(), connectSequentially, pendingRepair, previewKind);
   }

   public StreamPlan requestRanges(InetAddress from, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges) {
      return this.requestRanges(from, connecting, keyspace, ranges, EMPTY_COLUMN_FAMILIES);
   }

   public StreamPlan requestRanges(InetAddress from, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies) {
      StreamSession session = this.coordinator.getOrCreateNextSession(from, connecting);
      session.addStreamRequest(keyspace, ranges, Arrays.asList(columnFamilies));
      return this;
   }

   public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies) {
      return this.transferRanges(to, to, keyspace, ranges, columnFamilies);
   }

   public StreamPlan transferRanges(InetAddress to, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges) {
      return this.transferRanges(to, connecting, keyspace, ranges, EMPTY_COLUMN_FAMILIES);
   }

   public StreamPlan transferRanges(InetAddress to, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies) {
      StreamSession session = this.coordinator.getOrCreateNextSession(to, connecting);
      session.addTransferRanges(keyspace, ranges, Arrays.asList(columnFamilies), this.flushBeforeTransfer);
      return this;
   }

   public StreamPlan transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails) {
      this.coordinator.transferFiles(to, sstableDetails);
      return this;
   }

   public StreamPlan listeners(StreamEventHandler handler, StreamEventHandler... handlers) {
      this.handlers.add(handler);
      if(handlers != null) {
         Collections.addAll(this.handlers, handlers);
      }

      return this;
   }

   public StreamPlan connectionFactory(StreamConnectionFactory factory) {
      this.coordinator.setConnectionFactory(factory);
      return this;
   }

   public boolean isEmpty() {
      return !this.coordinator.hasActiveSessions();
   }

   public StreamResultFuture execute() {
      return StreamResultFuture.init(this.planId, this.streamOperation, this.handlers, this.coordinator);
   }

   public StreamPlan flushBeforeTransfer(boolean flushBeforeTransfer) {
      this.flushBeforeTransfer = flushBeforeTransfer;
      return this;
   }

   public UUID getPendingRepair() {
      return this.coordinator.getPendingRepair();
   }

   public boolean getFlushBeforeTransfer() {
      return this.flushBeforeTransfer;
   }
}
