package org.apache.cassandra.db.rows;

import io.reactivex.functions.Function;
import org.apache.cassandra.utils.flow.Flow;

public interface FlowablePartition extends FlowablePartitionBase<Row> {
   static default FlowablePartition create(PartitionHeader header, Row staticRow, Flow<Row> content) {
      return new FlowablePartition.Instance(header, staticRow, content);
   }

   default FlowablePartition withHeader(PartitionHeader header, Row staticRow) {
      return create(header, staticRow, this.content());
   }

   default FlowablePartition withContent(Flow<Row> content) {
      return create(this.header(), this.staticRow(), content);
   }

   default FlowablePartition mapContent(Function<Row, Row> mapper) {
      return new FlowablePartition.Map(this.content(), this.staticRow(), this.header(), mapper);
   }

   default FlowablePartition skippingMapContent(Function<Row, Row> mapper, Row staticRow) {
      return new FlowablePartition.SkippingMap(this.content(), staticRow, this.header(), mapper);
   }

   public static class SkippingMap extends Flow.SkippingMap<Row, Row> implements FlowablePartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected SkippingMap(Flow<Row> sourceContent, Row staticRow, PartitionHeader header, Function<Row, Row> mapper) {
         super(sourceContent, mapper);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Row> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public static class Map extends Flow.Map<Row, Row> implements FlowablePartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected Map(Flow<Row> sourceContent, Row staticRow, PartitionHeader header, Function<Row, Row> mapper) {
         super(sourceContent, mapper);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Row> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public abstract static class FlowSource extends org.apache.cassandra.utils.flow.FlowSource<Row> implements FlowablePartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected FlowSource(PartitionHeader header, Row staticRow) {
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Row> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public abstract static class FlowTransform extends org.apache.cassandra.utils.flow.FlowTransform<Row, Row> implements FlowablePartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected FlowTransform(Flow<Row> sourceContent, Row staticRow, PartitionHeader header) {
         super(sourceContent);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Row> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public abstract static class FlowTransformNext extends org.apache.cassandra.utils.flow.FlowTransformNext<Row, Row> implements FlowablePartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected FlowTransformNext(Flow<Row> sourceContent, Row staticRow, PartitionHeader header) {
         super(sourceContent);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Row> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public static class Instance implements FlowablePartition {
      private final PartitionHeader header;
      private final Row staticRow;
      private final Flow<Row> content;

      private Instance(PartitionHeader header, Row staticRow, Flow<Row> content) {
         this.header = header;
         this.staticRow = staticRow;
         this.content = content;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Row staticRow() {
         return this.staticRow;
      }

      public Flow<Row> content() {
         return this.content;
      }
   }
}
