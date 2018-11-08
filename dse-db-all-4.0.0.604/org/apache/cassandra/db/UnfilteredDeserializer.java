package org.apache.cassandra.db;

import java.io.IOException;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;

public class UnfilteredDeserializer {
   protected final TableMetadata metadata;
   protected final FileDataInput in;
   protected final SerializationHelper helper;
   private final ClusteringPrefix.Deserializer clusteringDeserializer;
   private final SerializationHeader header;
   private final UnfilteredSerializer serializer;
   private int nextFlags;
   private int nextExtendedFlags;
   private boolean isReady;
   private boolean isDone;
   private long preparePos;
   private final Row.Builder builder;

   private UnfilteredDeserializer(TableMetadata metadata, FileDataInput in, SerializationHeader header, SerializationHelper helper) {
      this.metadata = metadata;
      this.in = in;
      this.helper = helper;
      this.header = header;
      this.serializer = (UnfilteredSerializer)UnfilteredSerializer.serializers.get(helper.version);
      this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
      this.builder = Row.Builder.sorted();
      this.preparePos = -1L;
   }

   public static UnfilteredDeserializer create(TableMetadata metadata, FileDataInput in, SerializationHeader header, SerializationHelper helper) {
      return new UnfilteredDeserializer(metadata, in, header, helper);
   }

   public boolean hasNext() throws IOException {
      if(this.isReady) {
         return true;
      } else {
         this.prepareNext();
         return !this.isDone;
      }
   }

   private void prepareNext() throws IOException {
      if(!this.isDone) {
         this.preparePos = this.in.getFilePointer();
         this.nextFlags = this.in.readUnsignedByte();
         if(UnfilteredSerializer.isEndOfPartition(this.nextFlags)) {
            this.isDone = true;
            this.isReady = false;
         } else {
            this.nextExtendedFlags = UnfilteredSerializer.readExtendedFlags(this.in, this.nextFlags);
            this.clusteringDeserializer.prepare(this.nextFlags, this.nextExtendedFlags);
            this.isReady = true;
         }
      }
   }

   public void rewind() throws IOException {
      if(!this.isDone && this.isReady) {
         assert this.preparePos != -1L;

         this.in.seek(this.preparePos);
         this.preparePos = -1L;
         this.isReady = false;
      }
   }

   public int compareNextTo(ClusteringBound bound) throws IOException {
      if(!this.isReady) {
         this.prepareNext();
      }

      assert !this.isDone;

      return this.clusteringDeserializer.compareNextTo(bound);
   }

   public boolean nextIsRow() throws IOException {
      if(!this.isReady) {
         this.prepareNext();
      }

      return UnfilteredSerializer.kind(this.nextFlags) == Unfiltered.Kind.ROW;
   }

   public Unfiltered readNext() throws IOException {
      this.isReady = false;
      if(UnfilteredSerializer.kind(this.nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
         ClusteringBoundOrBoundary bound = this.clusteringDeserializer.deserializeNextBound();
         return this.serializer.deserializeMarkerBody(this.in, this.header, bound);
      } else {
         this.builder.newRow(this.clusteringDeserializer.deserializeNextClustering());
         return this.serializer.deserializeRowBody(this.in, this.header, this.helper, this.nextFlags, this.nextExtendedFlags, this.builder);
      }
   }

   public void clearState() {
      this.builder.reset();
      this.isReady = false;
      this.isDone = false;
   }

   public void skipNext() throws IOException {
      this.isReady = false;
      this.clusteringDeserializer.skipNext();
      if(UnfilteredSerializer.kind(this.nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
         this.serializer.skipMarkerBody(this.in);
      } else {
         this.serializer.skipRowBody(this.in);
      }

   }
}
