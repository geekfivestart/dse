package org.apache.cassandra.db.compaction;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

public final class CompactionInfo implements Serializable {
   private static final long serialVersionUID = 3695381572726744816L;
   private final TableMetadata metadata;
   private final OperationType tasktype;
   private final long completed;
   private final long total;
   private final CompactionInfo.Unit unit;
   private final UUID compactionId;

   public CompactionInfo(TableMetadata metadata, OperationType tasktype, long bytesComplete, long totalBytes, UUID compactionId) {
      this(metadata, tasktype, bytesComplete, totalBytes, CompactionInfo.Unit.BYTES, compactionId);
   }

   public CompactionInfo(OperationType tasktype, long completed, long total, CompactionInfo.Unit unit, UUID compactionId) {
      this((TableMetadata)null, tasktype, completed, total, unit, compactionId);
   }

   public CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, CompactionInfo.Unit unit, UUID compactionId) {
      this.tasktype = tasktype;
      this.completed = completed;
      this.total = total;
      this.metadata = metadata;
      this.unit = unit;
      this.compactionId = compactionId;
   }

   public CompactionInfo forProgress(long complete, long total) {
      return new CompactionInfo(this.metadata, this.tasktype, complete, total, this.unit, this.compactionId);
   }

   public UUID getId() {
      return this.metadata != null?this.metadata.id.asUUID():null;
   }

   public String getKeyspace() {
      return this.metadata != null?this.metadata.keyspace:null;
   }

   public String getColumnFamily() {
      return this.metadata != null?this.metadata.name:null;
   }

   public TableMetadata getTableMetadata() {
      return this.metadata;
   }

   public long getCompleted() {
      return this.completed;
   }

   public long getTotal() {
      return this.total;
   }

   public OperationType getTaskType() {
      return this.tasktype;
   }

   public UUID compactionId() {
      return this.compactionId;
   }

   public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append(this.getTaskType());
      if(this.metadata != null) {
         buff.append('@').append(this.getId()).append('(');
         buff.append(this.getKeyspace()).append(", ").append(this.getColumnFamily()).append(", ");
      } else {
         buff.append('(');
      }

      buff.append(this.getCompleted()).append('/').append(this.getTotal());
      return buff.append(')').append(this.unit).toString();
   }

   public Map<String, String> asMap() {
      Map<String, String> ret = new HashMap();
      ret.put("id", this.getId() == null?"":this.getId().toString());
      ret.put("keyspace", this.getKeyspace());
      ret.put("columnfamily", this.getColumnFamily());
      ret.put("completed", Long.toString(this.completed));
      ret.put("total", Long.toString(this.total));
      ret.put("taskType", this.tasktype.toString());
      ret.put("unit", this.unit.toString());
      ret.put("compactionId", this.compactionId == null?"":this.compactionId.toString());
      return ret;
   }

   public abstract static class Holder {
      private volatile boolean stopRequested = false;

      public Holder() {
      }

      public abstract CompactionInfo getCompactionInfo();

      public void stop() {
         this.stop(Predicates.alwaysTrue());
      }

      public void stop(Predicate<SSTableReader> predicate) {
         this.stopRequested = this.maybeStop(predicate);
      }

      public boolean isStopRequested() {
         return this.stopRequested;
      }

      protected boolean maybeStop(Predicate<SSTableReader> predicate) {
         return true;
      }
   }

   public static enum Unit {
      BYTES("bytes"),
      RANGES("token range parts"),
      KEYS("keys");

      private final String name;

      private Unit(String name) {
         this.name = name;
      }

      public String toString() {
         return this.name;
      }

      public static boolean isFileSize(String unit) {
         return BYTES.toString().equals(unit);
      }
   }
}
