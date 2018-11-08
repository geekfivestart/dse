package org.apache.cassandra.db.rows;

import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import org.apache.cassandra.utils.flow.Flow;

public interface FlowableUnfilteredPartition extends FlowablePartitionBase<Unfiltered> {
   static default FlowableUnfilteredPartition create(PartitionHeader header, Row staticRow, Flow<Unfiltered> content) {
      return new FlowableUnfilteredPartition.Instance(header, staticRow, content);
   }

   default FlowableUnfilteredPartition withHeader(PartitionHeader header, Row staticRow) {
      return create(header, staticRow, this.content());
   }

   default FlowableUnfilteredPartition withContent(Flow<Unfiltered> content) {
      return create(this.header(), this.staticRow(), content);
   }

   default FlowableUnfilteredPartition skipLowerBound() {
      return this;
   }

   default FlowableUnfilteredPartition mapContent(Function<Unfiltered, Unfiltered> mapper) {
      return new FlowableUnfilteredPartition.Map(this.content(), this.staticRow(), this.header(), mapper);
   }

   default FlowableUnfilteredPartition skippingMapContent(Function<Unfiltered, Unfiltered> mapper, Row staticRow) {
      return new FlowableUnfilteredPartition.SkippingMap(this.content(), staticRow, this.header(), mapper);
   }

   default FlowableUnfilteredPartition filterContent(Predicate<Unfiltered> tester) {
      return new FlowableUnfilteredPartition.Filter(this.content(), this.staticRow(), this.header(), tester);
   }

   public static class Filter extends Flow.Filter<Unfiltered> implements FlowableUnfilteredPartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected Filter(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header, Predicate<Unfiltered> tester) {
         super(sourceContent, tester);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Unfiltered> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public static class SkippingMap extends Flow.SkippingMap<Unfiltered, Unfiltered> implements FlowableUnfilteredPartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected SkippingMap(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header, Function<Unfiltered, Unfiltered> mapper) {
         super(sourceContent, mapper);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Unfiltered> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public static class Map extends Flow.Map<Unfiltered, Unfiltered> implements FlowableUnfilteredPartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected Map(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header, Function<Unfiltered, Unfiltered> mapper) {
         super(sourceContent, mapper);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Unfiltered> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public abstract static class FlowSource extends org.apache.cassandra.utils.flow.FlowSource<Unfiltered> implements FlowableUnfilteredPartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected FlowSource(PartitionHeader header, Row staticRow) {
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Unfiltered> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public abstract static class FlowTransform extends org.apache.cassandra.utils.flow.FlowTransform<Unfiltered, Unfiltered> implements FlowableUnfilteredPartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected FlowTransform(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header) {
         super(sourceContent);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Unfiltered> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public abstract static class FlowTransformNext extends org.apache.cassandra.utils.flow.FlowTransformNext<Unfiltered, Unfiltered> implements FlowableUnfilteredPartition {
      private final PartitionHeader header;
      private final Row staticRow;

      protected FlowTransformNext(Flow<Unfiltered> sourceContent, Row staticRow, PartitionHeader header) {
         super(sourceContent);
         this.header = header;
         this.staticRow = staticRow;
      }

      public PartitionHeader header() {
         return this.header;
      }

      public Flow<Unfiltered> content() {
         return this;
      }

      public Row staticRow() {
         return this.staticRow;
      }
   }

   public static class Instance implements FlowableUnfilteredPartition {
      private final PartitionHeader header;
      private final Row staticRow;
      private final Flow<Unfiltered> content;

      public Instance(PartitionHeader header, Row staticRow, Flow<Unfiltered> content) {
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

      public Flow<Unfiltered> content() {
         return this.content;
      }
   }
}
