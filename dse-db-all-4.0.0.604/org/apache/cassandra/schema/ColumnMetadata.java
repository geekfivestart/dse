package org.apache.cassandra.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;
import com.google.common.collect.UnmodifiableIterator;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.cql3.selection.SimpleSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MultiCellType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.github.jamm.Unmetered;

@Unmetered
public final class ColumnMetadata extends ColumnSpecification implements Selectable, Comparable<ColumnMetadata> {
   public static final Comparator<Object> asymmetricColumnDataComparator = (a, b) -> {
      return ((ColumnData)a).column().compareTo((ColumnMetadata)b);
   };
   public static final int NO_POSITION = -1;
   public final ColumnMetadata.Kind kind;
   private final int position;
   private final Comparator<CellPath> cellPathComparator;
   private final Comparator<Object> asymmetricCellPathComparator;
   private final Comparator<? super Cell> cellComparator;
   private int hash;
   private final long comparisonOrder;

   private static long comparisonOrder(ColumnMetadata.Kind kind, boolean isComplex, long position, ColumnIdentifier name) {
      assert position >= 0L && position < 4096L;

      return (long)kind.ordinal() << 61 | (isComplex?1152921504606846976L:0L) | position << 48 | name.prefixComparison >>> 16;
   }

   public static ColumnMetadata partitionKeyColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type, int position) {
      return new ColumnMetadata(table, name, type, position, ColumnMetadata.Kind.PARTITION_KEY);
   }

   public static ColumnMetadata partitionKeyColumn(String keyspace, String table, String name, AbstractType<?> type, int position) {
      return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, position, ColumnMetadata.Kind.PARTITION_KEY);
   }

   public static ColumnMetadata clusteringColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type, int position) {
      return new ColumnMetadata(table, name, type, position, ColumnMetadata.Kind.CLUSTERING);
   }

   public static ColumnMetadata clusteringColumn(String keyspace, String table, String name, AbstractType<?> type, int position) {
      return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, position, ColumnMetadata.Kind.CLUSTERING);
   }

   public static ColumnMetadata regularColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type) {
      return new ColumnMetadata(table, name, type, -1, ColumnMetadata.Kind.REGULAR);
   }

   public static ColumnMetadata regularColumn(String keyspace, String table, String name, AbstractType<?> type) {
      return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, -1, ColumnMetadata.Kind.REGULAR);
   }

   public static ColumnMetadata staticColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type) {
      return new ColumnMetadata(table, name, type, -1, ColumnMetadata.Kind.STATIC);
   }

   public static ColumnMetadata staticColumn(String keyspace, String table, String name, AbstractType<?> type) {
      return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, -1, ColumnMetadata.Kind.STATIC);
   }

   public ColumnMetadata(TableMetadata table, ByteBuffer name, AbstractType<?> type, int position, ColumnMetadata.Kind kind) {
      this(table.keyspace, table.name, ColumnIdentifier.getInterned(name, table.columnDefinitionNameComparator(kind)), type, position, kind);
   }

   @VisibleForTesting
   public ColumnMetadata(String ksName, String cfName, ColumnIdentifier name, AbstractType<?> type, int position, ColumnMetadata.Kind kind) {
      super(ksName, cfName, name, type);

      assert name != null && type != null && kind != null;

      assert name.isInterned();

      assert position == -1 == !kind.isPrimaryKeyKind();

      this.kind = kind;
      this.position = position;
      this.cellPathComparator = makeCellPathComparator(kind, type);
      this.cellComparator = this.cellPathComparator == null?ColumnData.comparator:(a, b) -> {
         return this.cellPathComparator.compare(a.path(), b.path());
      };
      this.asymmetricCellPathComparator = this.cellPathComparator == null?null:(a, b) -> {
         return this.cellPathComparator.compare(((Cell)a).path(), (CellPath)b);
      };
      this.comparisonOrder = comparisonOrder(kind, this.isComplex(), (long)Math.max(0, position), name);
   }

   private static Comparator<CellPath> makeCellPathComparator(ColumnMetadata.Kind kind, AbstractType<?> type) {
      if(!kind.isPrimaryKeyKind() && type.isMultiCell()) {
         AbstractType<?> nameComparator = ((MultiCellType)type).nameComparator();
         return (path1, path2) -> {
            if(path1.size() != 0 && path2.size() != 0) {
               assert path1.size() == 1 && path2.size() == 1;

               return nameComparator.compare(path1.get(0), path2.get(0));
            } else {
               return path1 == CellPath.BOTTOM?(path2 == CellPath.BOTTOM?0:-1):(path1 == CellPath.TOP?(path2 == CellPath.TOP?0:1):(path2 == CellPath.BOTTOM?1:-1));
            }
         };
      } else {
         return null;
      }
   }

   public ColumnMetadata copy() {
      return new ColumnMetadata(this.ksName, this.cfName, this.name, this.type, this.position, this.kind);
   }

   public ColumnMetadata withNewName(ColumnIdentifier newName) {
      return new ColumnMetadata(this.ksName, this.cfName, newName, this.type, this.position, this.kind);
   }

   public ColumnMetadata withNewType(AbstractType<?> newType) {
      return new ColumnMetadata(this.ksName, this.cfName, this.name, newType, this.position, this.kind);
   }

   public boolean isPartitionKey() {
      return this.kind == ColumnMetadata.Kind.PARTITION_KEY;
   }

   public boolean isClusteringColumn() {
      return this.kind == ColumnMetadata.Kind.CLUSTERING;
   }

   public boolean isStatic() {
      return this.kind == ColumnMetadata.Kind.STATIC;
   }

   public boolean isRegular() {
      return this.kind == ColumnMetadata.Kind.REGULAR;
   }

   public ColumnMetadata.ClusteringOrder clusteringOrder() {
      return !this.isClusteringColumn()?ColumnMetadata.ClusteringOrder.NONE:(this.type.isReversed()?ColumnMetadata.ClusteringOrder.DESC:ColumnMetadata.ClusteringOrder.ASC);
   }

   public int position() {
      return this.position;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ColumnMetadata)) {
         return false;
      } else {
         ColumnMetadata cd = (ColumnMetadata)o;
         return Objects.equals(this.ksName, cd.ksName) && Objects.equals(this.cfName, cd.cfName) && Objects.equals(this.name, cd.name) && Objects.equals(this.type, cd.type) && Objects.equals(this.kind, cd.kind) && Objects.equals(Integer.valueOf(this.position), Integer.valueOf(cd.position));
      }
   }

   public int hashCode() {
      int result = this.hash;
      if(result == 0) {
         result = 31 + (this.ksName == null?0:this.ksName.hashCode());
         result = 31 * result + (this.cfName == null?0:this.cfName.hashCode());
         result = 31 * result + (this.name == null?0:this.name.hashCode());
         result = 31 * result + (this.type == null?0:this.type.hashCode());
         result = 31 * result + (this.kind == null?0:this.kind.hashCode());
         result = 31 * result + this.position;
         this.hash = result;
      }

      return result;
   }

   public String toString() {
      return this.name.toString();
   }

   public String debugString() {
      return MoreObjects.toStringHelper(this).add("name", this.name).add("type", this.type).add("kind", this.kind).add("position", this.position).toString();
   }

   public boolean isPrimaryKeyColumn() {
      return this.kind.isPrimaryKeyKind();
   }

   public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
      return predicate.test(this);
   }

   public boolean processesSelection() {
      return false;
   }

   public static Collection<ColumnIdentifier> toIdentifiers(Collection<ColumnMetadata> definitions) {
      return Collections2.transform(definitions, (columnDef) -> {
         return columnDef.name;
      });
   }

   public int compareTo(ColumnMetadata other) {
      return this == other?0:(this.comparisonOrder != other.comparisonOrder?Long.compare(this.comparisonOrder, other.comparisonOrder):this.name.compareTo(other.name));
   }

   public Comparator<CellPath> cellPathComparator() {
      return this.cellPathComparator;
   }

   public Comparator<Object> asymmetricCellPathComparator() {
      return this.asymmetricCellPathComparator;
   }

   public Comparator<? super Cell> cellComparator() {
      return this.cellComparator;
   }

   public boolean isComplex() {
      return this.cellPathComparator != null;
   }

   public boolean isSimple() {
      return !this.isComplex();
   }

   public CellPath.Serializer cellPathSerializer() {
      return CollectionType.cellPathSerializer;
   }

   public void validateCell(Cell cell) {
      if(cell.isTombstone()) {
         if(cell.value().hasRemaining()) {
            throw new MarshalException("A tombstone should not have a value");
         }

         if(cell.path() != null) {
            this.validateCellPath(cell.path());
         }
      } else if(this.type.isUDT()) {
         ((UserType)this.type).validateCell(cell);
      } else {
         this.type.validateCellValue(cell.value());
         if(cell.path() != null) {
            this.validateCellPath(cell.path());
         }
      }

   }

   private void validateCellPath(CellPath path) {
      if(!this.isComplex()) {
         throw new MarshalException("Only complex cells should have a cell path");
      } else {
         assert this.type.isMultiCell();

         if(this.type.isCollection()) {
            ((CollectionType)this.type).nameComparator().validate(path.get(0));
         } else {
            ((UserType)this.type).nameComparator().validate(path.get(0));
         }

      }
   }

   public static String toCQLString(Iterable<ColumnMetadata> defs) {
      return toCQLString(defs.iterator());
   }

   public static String toCQLString(Iterator<ColumnMetadata> defs) {
      if(!defs.hasNext()) {
         return "";
      } else {
         StringBuilder sb = new StringBuilder();
         sb.append(((ColumnMetadata)defs.next()).name);

         while(defs.hasNext()) {
            sb.append(", ").append(((ColumnMetadata)defs.next()).name);
         }

         return sb.toString();
      }
   }

   public AbstractType<?> cellValueType() {
      assert !(this.type instanceof UserType) || !this.type.isMultiCell();

      return this.type instanceof CollectionType && this.type.isMultiCell()?((CollectionType)this.type).valueComparator():this.type;
   }

   public boolean isCounterColumn() {
      return this.type instanceof CollectionType?((CollectionType)this.type).valueComparator().isCounter():this.type.isCounter();
   }

   public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) throws InvalidRequestException {
      return SimpleSelector.newFactory(this, this.addAndGetIndex(this, defs));
   }

   public AbstractType<?> getExactTypeIfKnown(String keyspace) {
      return this.type;
   }

   public abstract static class Raw extends Selectable.Raw {
      public Raw() {
      }

      public static ColumnMetadata.Raw forUnquoted(String text) {
         return new ColumnMetadata.Raw.Literal(text, false);
      }

      public static ColumnMetadata.Raw forQuoted(String text) {
         return new ColumnMetadata.Raw.Literal(text, true);
      }

      public static ColumnMetadata.Raw forColumn(ColumnMetadata column) {
         return new ColumnMetadata.Raw.ForColumn(column);
      }

      public abstract ColumnIdentifier getIdentifier(TableMetadata var1);

      public abstract String rawText();

      public abstract ColumnMetadata prepare(TableMetadata var1);

      public final int hashCode() {
         return this.toString().hashCode();
      }

      public final boolean equals(Object o) {
         if(!(o instanceof ColumnMetadata.Raw)) {
            return false;
         } else {
            ColumnMetadata.Raw that = (ColumnMetadata.Raw)o;
            return this.toString().equals(that.toString());
         }
      }

      private static class ForColumn extends ColumnMetadata.Raw {
         private final ColumnMetadata column;

         private ForColumn(ColumnMetadata column) {
            this.column = column;
         }

         public ColumnIdentifier getIdentifier(TableMetadata table) {
            return this.column.name;
         }

         public ColumnMetadata prepare(TableMetadata table) {
            assert table.getColumn(this.column.name) != null;

            return this.column;
         }

         public String rawText() {
            return this.column.name.toString();
         }

         public String toString() {
            return this.column.name.toCQLString();
         }
      }

      private static class Literal extends ColumnMetadata.Raw {
         private final String text;

         public Literal(String rawText, boolean keepCase) {
            this.text = keepCase?rawText:rawText.toLowerCase(Locale.US);
         }

         public ColumnIdentifier getIdentifier(TableMetadata table) {
            if(!table.isStaticCompactTable()) {
               return ColumnIdentifier.getInterned(this.text, true);
            } else {
               AbstractType<?> columnNameType = table.staticCompactOrSuperTableColumnNameType();
               if(columnNameType instanceof UTF8Type) {
                  return ColumnIdentifier.getInterned(this.text, true);
               } else {
                  ByteBuffer bufferName = ByteBufferUtil.bytes(this.text);
                  UnmodifiableIterator var4 = table.columns().iterator();

                  ColumnMetadata def;
                  do {
                     if(!var4.hasNext()) {
                        return ColumnIdentifier.getInterned(columnNameType, columnNameType.fromString(this.text), this.text);
                     }

                     def = (ColumnMetadata)var4.next();
                  } while(!def.name.bytes.equals(bufferName));

                  return def.name;
               }
            }
         }

         public ColumnMetadata prepare(TableMetadata table) {
            if(!table.isStaticCompactTable()) {
               return this.find(table);
            } else {
               AbstractType<?> columnNameType = table.staticCompactOrSuperTableColumnNameType();
               if(columnNameType instanceof UTF8Type) {
                  return this.find(table);
               } else {
                  ByteBuffer bufferName = ByteBufferUtil.bytes(this.text);
                  UnmodifiableIterator var4 = table.columns().iterator();

                  ColumnMetadata def;
                  do {
                     if(!var4.hasNext()) {
                        return this.find(columnNameType.fromString(this.text), table);
                     }

                     def = (ColumnMetadata)var4.next();
                  } while(!def.name.bytes.equals(bufferName));

                  return def;
               }
            }
         }

         private ColumnMetadata find(TableMetadata table) {
            return this.find(ByteBufferUtil.bytes(this.text), table);
         }

         private ColumnMetadata find(ByteBuffer id, TableMetadata table) {
            ColumnMetadata def = table.getColumn(id);
            if(def == null) {
               throw new InvalidRequestException(String.format("Undefined column name %s", new Object[]{this.toString()}));
            } else {
               return def;
            }
         }

         public String rawText() {
            return this.text;
         }

         public String toString() {
            return ColumnIdentifier.maybeQuote(this.text);
         }
      }
   }

   public static enum Kind {
      PARTITION_KEY,
      CLUSTERING,
      REGULAR,
      STATIC;

      private Kind() {
      }

      public boolean isPrimaryKeyKind() {
         return this == PARTITION_KEY || this == CLUSTERING;
      }
   }

   public static enum ClusteringOrder {
      ASC,
      DESC,
      NONE;

      private ClusteringOrder() {
      }
   }
}
