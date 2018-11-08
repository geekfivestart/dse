package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CollectionType<T> extends AbstractType<T> implements MultiCellType {
   public static CellPath.Serializer cellPathSerializer = new CollectionType.CollectionPathSerializer();
   public final CollectionType.Kind kind;

   protected CollectionType(AbstractType.ComparisonType comparisonType, CollectionType.Kind kind) {
      super(comparisonType, -1);
      this.kind = kind;
   }

   public abstract AbstractType<?> nameComparator();

   public abstract AbstractType<?> valueComparator();

   protected abstract List<ByteBuffer> serializedValues(Iterator<Cell> var1);

   public abstract CollectionSerializer<T> getSerializer();

   public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey) {
      return this.kind.makeCollectionReceiver(collection, isKey);
   }

   public String getString(ByteBuffer bytes) {
      return BytesType.instance.getString(bytes);
   }

   public String getString(CellPath path, ByteBuffer bytes) {
      assert this.isMultiCell() : "Should only be called when isMultiCell() returns true";

      return String.format("[%s]=%s", new Object[]{this.nameComparator().getString(path.get(0)), this.valueComparator().getString(bytes)});
   }

   public ByteBuffer fromString(String source) {
      try {
         return ByteBufferUtil.hexToBytes(source);
      } catch (NumberFormatException var3) {
         throw new MarshalException(String.format("cannot parse '%s' as hex bytes", new Object[]{source}), var3);
      }
   }

   public boolean isCollection() {
      return true;
   }

   public void validateCellValue(ByteBuffer cellValue) throws MarshalException {
      if(this.isMultiCell()) {
         this.valueComparator().validateCellValue(cellValue);
      } else {
         super.validateCellValue(cellValue);
      }

   }

   public boolean isMap() {
      return this.kind == CollectionType.Kind.MAP;
   }

   public boolean isFreezable() {
      return true;
   }

   protected int collectionSize(List<ByteBuffer> values) {
      return values.size();
   }

   public ByteBuffer serializeForNativeProtocol(Iterator<Cell> cells, ProtocolVersion version) {
      assert this.isMultiCell();

      List<ByteBuffer> values = this.serializedValues(cells);
      int size = this.collectionSize(values);
      return CollectionSerializer.pack(values, size, version);
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      if(this == previous) {
         return true;
      } else if(!this.getClass().equals(previous.getClass())) {
         return false;
      } else {
         CollectionType tprev = (CollectionType)previous;
         return this.isMultiCell() != tprev.isMultiCell()?false:(!this.isMultiCell()?this.isCompatibleWithFrozen(tprev):(!this.nameComparator().isCompatibleWith(tprev.nameComparator())?false:this.valueComparator().isValueCompatibleWith(tprev.valueComparator())));
      }
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> previous) {
      if(this.isMultiCell()) {
         return this.isCompatibleWith(previous);
      } else if(this == previous) {
         return true;
      } else if(!this.getClass().equals(previous.getClass())) {
         return false;
      } else {
         CollectionType tprev = (CollectionType)previous;
         return this.isMultiCell() != tprev.isMultiCell()?false:this.isValueCompatibleWithFrozen(tprev);
      }
   }

   protected abstract boolean isCompatibleWithFrozen(CollectionType<?> var1);

   protected abstract boolean isValueCompatibleWithFrozen(CollectionType<?> var1);

   public CQL3Type asCQL3Type() {
      return new CQL3Type.Collection(this);
   }

   public boolean equals(Object o, boolean ignoreFreezing) {
      if(this == o) {
         return true;
      } else if(!(o instanceof CollectionType)) {
         return false;
      } else {
         CollectionType other = (CollectionType)o;
         return this.kind != other.kind?false:(!ignoreFreezing && this.isMultiCell() != other.isMultiCell()?false:this.nameComparator().equals(other.nameComparator(), ignoreFreezing) && this.valueComparator().equals(other.valueComparator(), ignoreFreezing));
      }
   }

   public String toString() {
      return this.toString(false);
   }

   private static class CollectionPathSerializer implements CellPath.Serializer {
      private CollectionPathSerializer() {
      }

      public void serialize(CellPath path, DataOutputPlus out) throws IOException {
         ByteBufferUtil.writeWithVIntLength(path.get(0), out);
      }

      public CellPath deserialize(DataInputPlus in) throws IOException {
         return CellPath.create(ByteBufferUtil.readWithVIntLength(in));
      }

      public long serializedSize(CellPath path) {
         return (long)ByteBufferUtil.serializedSizeWithVIntLength(path.get(0));
      }

      public void skip(DataInputPlus in) throws IOException {
         ByteBufferUtil.skipWithVIntLength(in);
      }
   }

   public static enum Kind {
      MAP {
         public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey) {
            return isKey?Maps.keySpecOf(collection):Maps.valueSpecOf(collection);
         }
      },
      SET {
         public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey) {
            return Sets.valueSpecOf(collection);
         }
      },
      LIST {
         public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey) {
            return Lists.valueSpecOf(collection);
         }
      };

      private Kind() {
      }

      public abstract ColumnSpecification makeCollectionReceiver(ColumnSpecification var1, boolean var2);
   }
}
