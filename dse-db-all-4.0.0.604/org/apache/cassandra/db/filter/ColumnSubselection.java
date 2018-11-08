package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.Function;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class ColumnSubselection implements Comparable<ColumnSubselection> {
   public static final Versioned<ReadVerbs.ReadVersion, ColumnSubselection.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new ColumnSubselection.Serializer(x$0, null);
   });
   protected final ColumnMetadata column;

   protected ColumnSubselection(ColumnMetadata column) {
      this.column = column;
   }

   public static ColumnSubselection slice(ColumnMetadata column, CellPath from, CellPath to) {
      assert column.isComplex() && column.type instanceof CollectionType;

      assert from.size() <= 1 && to.size() <= 1;

      return new ColumnSubselection.Slice(column, from, to, null);
   }

   public static ColumnSubselection element(ColumnMetadata column, CellPath elt) {
      assert column.isComplex() && column.type instanceof CollectionType;

      assert elt.size() == 1;

      return new ColumnSubselection.Element(column, elt, null);
   }

   public ColumnMetadata column() {
      return this.column;
   }

   protected abstract ColumnSubselection.Kind kind();

   protected abstract CellPath comparisonPath();

   public int compareTo(ColumnSubselection other) {
      assert other.column().name.equals(this.column().name);

      return this.column().cellPathComparator().compare(this.comparisonPath(), other.comparisonPath());
   }

   public abstract int compareInclusionOf(CellPath var1);

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(ColumnSubselection subSel, DataOutputPlus out) throws IOException {
         ColumnMetadata column = subSel.column();
         ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
         out.writeByte(subSel.kind().ordinal());
         switch(null.$SwitchMap$org$apache$cassandra$db$filter$ColumnSubselection$Kind[subSel.kind().ordinal()]) {
         case 1:
            ColumnSubselection.Slice slice = (ColumnSubselection.Slice)subSel;
            column.cellPathSerializer().serialize(slice.from, out);
            column.cellPathSerializer().serialize(slice.to, out);
            break;
         case 2:
            ColumnSubselection.Element eltSelection = (ColumnSubselection.Element)subSel;
            column.cellPathSerializer().serialize(eltSelection.element, out);
            break;
         default:
            throw new AssertionError();
         }

      }

      public ColumnSubselection deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
         ColumnMetadata column = metadata.getColumn(name);
         if(column == null) {
            column = metadata.getDroppedColumn(name);
            if(column == null) {
               throw new UnknownColumnException(metadata, name);
            }
         }

         ColumnSubselection.Kind kind = ColumnSubselection.Kind.values()[in.readUnsignedByte()];
         switch(null.$SwitchMap$org$apache$cassandra$db$filter$ColumnSubselection$Kind[kind.ordinal()]) {
         case 1:
            CellPath from = column.cellPathSerializer().deserialize(in);
            CellPath to = column.cellPathSerializer().deserialize(in);
            return new ColumnSubselection.Slice(column, from, to, null);
         case 2:
            CellPath elt = column.cellPathSerializer().deserialize(in);
            return new ColumnSubselection.Element(column, elt, null);
         default:
            throw new AssertionError();
         }
      }

      public long serializedSize(ColumnSubselection subSel) {
         long size = 0L;
         ColumnMetadata column = subSel.column();
         size += (long)TypeSizes.sizeofWithShortLength(column.name.bytes);
         ++size;
         switch(null.$SwitchMap$org$apache$cassandra$db$filter$ColumnSubselection$Kind[subSel.kind().ordinal()]) {
         case 1:
            ColumnSubselection.Slice slice = (ColumnSubselection.Slice)subSel;
            size += column.cellPathSerializer().serializedSize(slice.from);
            size += column.cellPathSerializer().serializedSize(slice.to);
            break;
         case 2:
            ColumnSubselection.Element element = (ColumnSubselection.Element)subSel;
            size += column.cellPathSerializer().serializedSize(element.element);
         }

         return size;
      }
   }

   private static class Element extends ColumnSubselection {
      private final CellPath element;

      private Element(ColumnMetadata column, CellPath elt) {
         super(column);
         this.element = elt;
      }

      protected ColumnSubselection.Kind kind() {
         return ColumnSubselection.Kind.ELEMENT;
      }

      public CellPath comparisonPath() {
         return this.element;
      }

      public int compareInclusionOf(CellPath path) {
         return this.column.cellPathComparator().compare(path, this.element);
      }

      public String toString() {
         AbstractType<?> type = ((CollectionType)this.column().type).nameComparator();
         return String.format("[%s]", new Object[]{type.getString(this.element.get(0))});
      }
   }

   private static class Slice extends ColumnSubselection {
      private final CellPath from;
      private final CellPath to;

      private Slice(ColumnMetadata column, CellPath from, CellPath to) {
         super(column);
         this.from = from;
         this.to = to;
      }

      protected ColumnSubselection.Kind kind() {
         return ColumnSubselection.Kind.SLICE;
      }

      public CellPath comparisonPath() {
         return this.from;
      }

      public int compareInclusionOf(CellPath path) {
         Comparator<CellPath> cmp = this.column.cellPathComparator();
         return cmp.compare(path, this.from) < 0?-1:(cmp.compare(this.to, path) < 0?1:0);
      }

      public String toString() {
         AbstractType<?> type = ((CollectionType)this.column().type).nameComparator();
         return String.format("[%s:%s]", new Object[]{this.from == CellPath.BOTTOM?"":type.getString(this.from.get(0)), this.to == CellPath.TOP?"":type.getString(this.to.get(0))});
      }
   }

   private static enum Kind {
      SLICE,
      ELEMENT;

      private Kind() {
      }
   }
}
