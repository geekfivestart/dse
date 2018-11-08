package org.apache.cassandra.db;

import com.datastax.bdp.db.nodesync.Segment;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class NodeSyncReadCommand extends PartitionRangeReadCommand {
   private static final ReadCommand.SelectionDeserializer<NodeSyncReadCommand> selectionDeserializer = new NodeSyncReadCommand.Deserializer();
   public static final Versioned<ReadVerbs.ReadVersion, Serializer<NodeSyncReadCommand>> serializers = ReadVerbs.ReadVersion.versioned((v) -> {
      return new ReadCommand.ReadCommandSerializer(v, selectionDeserializer);
   });
   @Nullable
   private final StagedScheduler nodeSyncScheduler;

   private NodeSyncReadCommand(DigestVersion digestVersion, TableMetadata table, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange range, IndexMetadata index, StagedScheduler nodeSyncScheduler, TPCTaskType readType) {
      super(digestVersion, table, nowInSec, columnFilter, rowFilter, limits, range, index, nodeSyncScheduler, readType);
      this.nodeSyncScheduler = nodeSyncScheduler;
   }

   public NodeSyncReadCommand(Segment segment, int nowInSec, StagedScheduler nodeSyncScheduler) {
      this((DigestVersion)null, segment.table, nowInSec, ColumnFilter.all(segment.table), RowFilter.NONE, DataLimits.NONE, DataRange.forTokenRange(segment.range), (IndexMetadata)null, nodeSyncScheduler, TPCTaskType.READ_RANGE_NODESYNC);
   }

   public Supplier<StagedScheduler> getSchedulerSupplier() {
      return this.nodeSyncScheduler == null?super.getSchedulerSupplier():() -> {
         return this.nodeSyncScheduler;
      };
   }

   protected PartitionRangeReadCommand copy(DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange, IndexMetadata index, StagedScheduler scheduler) {
      return new NodeSyncReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, index, this.nodeSyncScheduler == null?scheduler:this.nodeSyncScheduler, TPCTaskType.READ_RANGE_NODESYNC);
   }

   protected boolean shouldRespectTombstoneThresholds() {
      return false;
   }

   public Request.Dispatcher<NodeSyncReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> endpoints) {
      return Verbs.READS.NODESYNC.newDispatcher(endpoints, this);
   }

   public Request<NodeSyncReadCommand, ReadResponse> requestTo(InetAddress endpoint) {
      return Verbs.READS.NODESYNC.newRequest(endpoint, this);
   }

   private static class Deserializer extends ReadCommand.SelectionDeserializer<NodeSyncReadCommand> {
      private Deserializer() {
      }

      public NodeSyncReadCommand deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, IndexMetadata index) throws IOException {
         DataRange range = ((DataRange.Serializer)DataRange.serializers.get(version)).deserialize(in, metadata);
         return new NodeSyncReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, range, (IndexMetadata)null, (StagedScheduler)null, TPCTaskType.READ_RANGE_NODESYNC);
      }
   }
}
