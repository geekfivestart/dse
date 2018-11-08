package org.apache.cassandra.db.aggregation;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class AggregationSpecification {
   public static final Versioned<ReadVerbs.ReadVersion, AggregationSpecification.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new AggregationSpecification.Serializer(x$0, null);
   });
   public static final AggregationSpecification.Factory AGGREGATE_EVERYTHING_FACTORY = new AggregationSpecification.Factory() {
      public AggregationSpecification newInstance(QueryOptions options) {
         return AggregationSpecification.AGGREGATE_EVERYTHING;
      }
   };
   public static final AggregationSpecification AGGREGATE_EVERYTHING;
   private final AggregationSpecification.Kind kind;

   public AggregationSpecification.Kind kind() {
      return this.kind;
   }

   private AggregationSpecification(AggregationSpecification.Kind kind) {
      this.kind = kind;
   }

   public final GroupMaker newGroupMaker() {
      return this.newGroupMaker(GroupingState.EMPTY_STATE);
   }

   public abstract GroupMaker newGroupMaker(GroupingState var1);

   public static AggregationSpecification.Factory aggregatePkPrefixFactory(final ClusteringComparator comparator, final int clusteringPrefixSize) {
      return new AggregationSpecification.Factory() {
         public AggregationSpecification newInstance(QueryOptions options) {
            return new AggregationSpecification.AggregateByPkPrefix(comparator, clusteringPrefixSize);
         }
      };
   }

   public static AggregationSpecification.Factory aggregatePkPrefixFactoryWithSelector(final ClusteringComparator comparator, final int clusteringPrefixSize, final Selector.Factory factory) {
      return new AggregationSpecification.Factory() {
         public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
            factory.addFunctionsTo(functions);
         }

         public AggregationSpecification newInstance(QueryOptions options) {
            Selector selector = factory.newInstance(options);
            selector.validateForGroupBy();
            return new AggregationSpecification.AggregateByPkPrefixWithSelector(comparator, clusteringPrefixSize, selector);
         }
      };
   }

   static {
      AGGREGATE_EVERYTHING = new AggregationSpecification(AggregationSpecification.Kind.AGGREGATE_EVERYTHING) {
         public GroupMaker newGroupMaker(GroupingState state) {
            return GroupMaker.GROUP_EVERYTHING;
         }
      };
   }

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(AggregationSpecification aggregationSpec, DataOutputPlus out) throws IOException {
         out.writeByte(aggregationSpec.kind().ordinal());
         switch(null.$SwitchMap$org$apache$cassandra$db$aggregation$AggregationSpecification$Kind[aggregationSpec.kind().ordinal()]) {
         case 1:
            break;
         case 2:
            out.writeUnsignedVInt((long)((AggregationSpecification.AggregateByPkPrefix)aggregationSpec).clusteringPrefixSize);
            break;
         case 3:
            AggregationSpecification.AggregateByPkPrefixWithSelector spec = (AggregationSpecification.AggregateByPkPrefixWithSelector)aggregationSpec;
            out.writeUnsignedVInt((long)spec.clusteringPrefixSize);
            ((Selector.Serializer)Selector.serializers.get(this.version)).serialize(spec.selector, out);
            break;
         default:
            throw new AssertionError();
         }

      }

      public AggregationSpecification deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         AggregationSpecification.Kind kind = AggregationSpecification.Kind.values()[in.readUnsignedByte()];
         switch(null.$SwitchMap$org$apache$cassandra$db$aggregation$AggregationSpecification$Kind[kind.ordinal()]) {
         case 1:
            return AggregationSpecification.AGGREGATE_EVERYTHING;
         case 2:
            return new AggregationSpecification.AggregateByPkPrefix(metadata.comparator, (int)in.readUnsignedVInt());
         case 3:
            int clusteringPrefixSize = (int)in.readUnsignedVInt();
            Selector selector = ((Selector.Serializer)Selector.serializers.get(this.version)).deserialize(in, metadata);
            return new AggregationSpecification.AggregateByPkPrefixWithSelector(metadata.comparator, clusteringPrefixSize, selector);
         default:
            throw new AssertionError();
         }
      }

      public long serializedSize(AggregationSpecification aggregationSpec) {
         long size = (long)TypeSizes.sizeof((byte)aggregationSpec.kind().ordinal());
         switch(null.$SwitchMap$org$apache$cassandra$db$aggregation$AggregationSpecification$Kind[aggregationSpec.kind().ordinal()]) {
         case 1:
            break;
         case 2:
            size += (long)TypeSizes.sizeofUnsignedVInt((long)((AggregationSpecification.AggregateByPkPrefix)aggregationSpec).clusteringPrefixSize);
            break;
         case 3:
            AggregationSpecification.AggregateByPkPrefixWithSelector spec = (AggregationSpecification.AggregateByPkPrefixWithSelector)aggregationSpec;
            size += (long)TypeSizes.sizeofUnsignedVInt((long)spec.clusteringPrefixSize);
            size += (long)((Selector.Serializer)Selector.serializers.get(this.version)).serializedSize(spec.selector);
            break;
         default:
            throw new AssertionError();
         }

         return size;
      }
   }

   private static final class AggregateByPkPrefixWithSelector extends AggregationSpecification.AggregateByPkPrefix {
      private final Selector selector;

      public AggregateByPkPrefixWithSelector(ClusteringComparator comparator, int clusteringPrefixSize, Selector selector) {
         super(AggregationSpecification.Kind.AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR, comparator, clusteringPrefixSize);
         this.selector = selector;
      }

      public GroupMaker newGroupMaker(GroupingState state) {
         return GroupMaker.newSelectorGroupMaker(this.comparator, this.clusteringPrefixSize, this.selector, state);
      }
   }

   private static class AggregateByPkPrefix extends AggregationSpecification {
      protected final int clusteringPrefixSize;
      protected final ClusteringComparator comparator;

      public AggregateByPkPrefix(ClusteringComparator comparator, int clusteringPrefixSize) {
         this(AggregationSpecification.Kind.AGGREGATE_BY_PK_PREFIX, comparator, clusteringPrefixSize);
      }

      protected AggregateByPkPrefix(AggregationSpecification.Kind kind, ClusteringComparator comparator, int clusteringPrefixSize) {
         super(kind, null);
         this.comparator = comparator;
         this.clusteringPrefixSize = clusteringPrefixSize;
      }

      public GroupMaker newGroupMaker(GroupingState state) {
         return GroupMaker.newPkPrefixGroupMaker(this.comparator, this.clusteringPrefixSize, state);
      }

      public boolean equals(Object other) {
         if(!(other instanceof AggregationSpecification.AggregateByPkPrefix)) {
            return false;
         } else {
            AggregationSpecification.AggregateByPkPrefix that = (AggregationSpecification.AggregateByPkPrefix)other;
            return this.clusteringPrefixSize == that.clusteringPrefixSize;
         }
      }
   }

   public interface Factory {
      AggregationSpecification newInstance(QueryOptions var1);

      default void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
      }
   }

   public static enum Kind {
      AGGREGATE_EVERYTHING,
      AGGREGATE_BY_PK_PREFIX,
      AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR;

      private Kind() {
      }
   }
}
