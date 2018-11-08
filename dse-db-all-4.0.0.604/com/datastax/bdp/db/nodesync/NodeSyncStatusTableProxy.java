package com.datastax.bdp.db.nodesync;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;

public interface NodeSyncStatusTableProxy {
   NodeSyncStatusTableProxy DEFAULT = new NodeSyncStatusTableProxy() {
      public List<NodeSyncRecord> nodeSyncRecords(TableMetadata table, Range<Token> range) {
         return SystemDistributedKeyspace.nodeSyncRecords(table, range);
      }

      public void lockNodeSyncSegment(Segment segment, long timeout, TimeUnit timeoutUnit) {
         SystemDistributedKeyspace.lockNodeSyncSegment(segment, timeout, timeoutUnit);
      }

      public void forceReleaseNodeSyncSegmentLock(Segment segment) {
         SystemDistributedKeyspace.forceReleaseNodeSyncSegmentLock(segment);
      }

      public void recordNodeSyncValidation(Segment segment, ValidationInfo info, boolean wasPreviousSuccessful) {
         SystemDistributedKeyspace.recordNodeSyncValidation(segment, info, wasPreviousSuccessful);
      }
   };

   List<NodeSyncRecord> nodeSyncRecords(TableMetadata var1, Range<Token> var2);

   default List<NodeSyncRecord> nodeSyncRecords(Segment segment) {
      return this.nodeSyncRecords(segment.table, segment.range);
   }

   void lockNodeSyncSegment(Segment var1, long var2, TimeUnit var4);

   void forceReleaseNodeSyncSegmentLock(Segment var1);

   void recordNodeSyncValidation(Segment var1, ValidationInfo var2, boolean var3);
}
