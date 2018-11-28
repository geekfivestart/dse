package org.apache.cassandra.db.filter;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnFilter {
   private static final Logger logger = LoggerFactory.getLogger(ColumnFilter.class);
   private static final NoSpamLogger noSpamLogger;
   public static final Versioned<ReadVerbs.ReadVersion, ColumnFilter.Serializer> serializers;
   final ColumnFilter.FetchType fetchType;
   final RegularAndStaticColumns fetched;
   final RegularAndStaticColumns queried;
   private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections;

   private ColumnFilter(ColumnFilter.FetchType fetchType, TableMetadata metadata, RegularAndStaticColumns queried, SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections) {
      if(fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED) {
         assert metadata != null : "Metadata is required if fetch type is not a selection";

         RegularAndStaticColumns all = metadata.regularAndStaticColumns();
         this.fetched = !all.statics.isEmpty() && queried != null?new RegularAndStaticColumns(queried.statics, all.regulars):all;
      } else {
         assert queried != null : "Queried columns are required if fetch type is a selection";

         this.fetched = queried;
      }

      this.fetchType = fetchType;
      this.queried = queried;
      this.subSelections = subSelections;
   }

   private ColumnFilter(ColumnFilter.FetchType fetchType, RegularAndStaticColumns fetched, RegularAndStaticColumns queried, SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections) {
      this.fetchType = fetchType;
      this.fetched = fetched;
      this.queried = queried;
      this.subSelections = subSelections;
      switch (fetchType) {
         case ALL_COLUMNS:
         case COLUMNS_IN_FETCHED: {
            assert fetched != null : "When fetching all columns, the _fetched_ set is required";
            break;
         }
         case COLUMNS_IN_QUERIED: {
            assert queried != null : "When fetching a COLUMNS_IN_QUERIED, the _queried_ set is required";
            assert queried == fetched : "When fetching a COLUMNS_IN_QUERIED both _fetched_ and _queried_ must point to the same set of columns";
            break;
         }
      }
   }

   public static ColumnFilter all(TableMetadata metadata) {
      return new ColumnFilter(ColumnFilter.FetchType.ALL_COLUMNS, metadata, (RegularAndStaticColumns)null, (SortedSetMultimap)null);
   }

   public static ColumnFilter selection(RegularAndStaticColumns columns) {
      return new ColumnFilter(ColumnFilter.FetchType.COLUMNS_IN_QUERIED, (TableMetadata)null, columns, (SortedSetMultimap)null);
   }

   public static ColumnFilter selection(TableMetadata metadata, RegularAndStaticColumns queried) {
      return new ColumnFilter(ColumnFilter.FetchType.ALL_COLUMNS, metadata, queried, (SortedSetMultimap)null);
   }

   public ColumnFilter withPartitionColumnsVerified(RegularAndStaticColumns partitionColumns) {
      if(this.fetchType == ColumnFilter.FetchType.ALL_COLUMNS && !this.fetched.includes(partitionColumns)) {
         noSpamLogger.info("Columns mismatch: `{}` does not include `{}`, falling back to the original set of columns.", new Object[]{this.fetched, partitionColumns});
         return new ColumnFilter(ColumnFilter.FetchType.COLUMNS_IN_FETCHED, this.fetched, this.queried, this.subSelections);
      } else {
         return this;
      }
   }

   public RegularAndStaticColumns fetchedColumns() {
      return this.fetched;
   }

   public RegularAndStaticColumns queriedColumns() {
      return this.queried == null?this.fetched:this.queried;
   }

   public boolean fetchesAllColumns(boolean isStatic) {
      switch (this.fetchType) {
         case ALL_COLUMNS: {
            return !isStatic || this.queried == null;
         }
         case COLUMNS_IN_FETCHED:
         case COLUMNS_IN_QUERIED: {
            return false;
         }
         default: {
            throw new IllegalStateException("Unrecognized fetch type: " + this.fetchType);
         }
      }
   }

   public boolean allFetchedColumnsAreQueried() {
      return this.fetchType == ColumnFilter.FetchType.COLUMNS_IN_QUERIED || this.queried == null;
   }

   public boolean fetches(ColumnMetadata column) {
      switch (this.fetchType) {
         case ALL_COLUMNS: {
            return !column.isStatic() || this.queried == null || this.queried.contains(column);
         }
         case COLUMNS_IN_FETCHED: {
            return this.fetched.contains(column);
         }
         case COLUMNS_IN_QUERIED: {
            return this.queried.contains(column);
         }
         default: {
            throw new IllegalStateException("Unrecognized fetch type: " + this.fetchType);
         }
      }
   }

   public boolean fetchedColumnIsQueried(ColumnMetadata column) {
      return this.fetchType == ColumnFilter.FetchType.COLUMNS_IN_QUERIED || this.queried == null || this.queried.contains(column);
   }

   public boolean fetchedCellIsQueried(ColumnMetadata column, CellPath path) {
      assert path != null;

      if(this.fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED && this.subSelections != null) {
         SortedSet<ColumnSubselection> s = this.subSelections.get(column.name);
         if(s.isEmpty()) {
            return true;
         } else {
            Iterator var4 = s.iterator();

            ColumnSubselection subSel;
            do {
               if(!var4.hasNext()) {
                  return false;
               }

               subSel = (ColumnSubselection)var4.next();
            } while(subSel.compareInclusionOf(path) != 0);

            return true;
         }
      } else {
         return true;
      }
   }

   public ColumnFilter.Tester newTester(ColumnMetadata column) {
      if(this.subSelections != null && column.isComplex()) {
         SortedSet<ColumnSubselection> s = this.subSelections.get(column.name);
         return s.isEmpty()?null:new ColumnFilter.Tester(!column.isStatic() && this.fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED, s.iterator());
      } else {
         return null;
      }
   }

   public Iterator<Cell> filterComplexCells(ColumnMetadata column, Iterator<Cell> cells) {
      ColumnFilter.Tester tester = this.newTester(column);
      return (Iterator)(tester == null?cells:Iterators.filter(cells, (cell) -> {
         return tester.fetchedCellIsQueried(cell.path());
      }));
   }

   public static ColumnFilter.Builder allRegularColumnsBuilder(TableMetadata metadata) {
      return new ColumnFilter.Builder(metadata);
   }

   public static ColumnFilter.Builder selectionBuilder() {
      return new ColumnFilter.Builder((TableMetadata)null);
   }

   public boolean equals(Object other) {
      if(other == this) {
         return true;
      } else if(!(other instanceof ColumnFilter)) {
         return false;
      } else {
         ColumnFilter otherCf = (ColumnFilter)other;
         return otherCf.fetchType == this.fetchType && Objects.equals(otherCf.fetched, this.fetched) && Objects.equals(otherCf.queried, this.queried) && Objects.equals(otherCf.subSelections, this.subSelections);
      }
   }

   public String toString() {
      if(this.fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED && this.queried == null) {
         return "*";
      } else if(this.queried.isEmpty()) {
         return "";
      } else {
         Iterator<ColumnMetadata> defs = this.queried.selectOrderIterator();
         if(!defs.hasNext()) {
            return "<none>";
         } else {
            StringBuilder sb = new StringBuilder();

            while(defs.hasNext()) {
               this.appendColumnDef(sb, (ColumnMetadata)defs.next());
               if(defs.hasNext()) {
                  sb.append(", ");
               }
            }

            return sb.toString();
         }
      }
   }

   private void appendColumnDef(StringBuilder sb, ColumnMetadata column) {
      if(this.subSelections == null) {
         sb.append(column.name);
      } else {
         SortedSet<ColumnSubselection> s = this.subSelections.get(column.name);
         if(s.isEmpty()) {
            sb.append(column.name);
         } else {
            int i = 0;
            Iterator var5 = s.iterator();

            while(var5.hasNext()) {
               ColumnSubselection subSel = (ColumnSubselection)var5.next();
               sb.append(i++ == 0?"":", ").append(column.name).append(subSel);
            }

         }
      }
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);
      serializers = ReadVerbs.ReadVersion.versioned((ver) -> {
         return new ColumnFilter.Serializer(ver);
      });
   }

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private static final int FETCH_ALL_MASK = 1;
      private static final int HAS_QUERIED_MASK = 2;
      private static final int HAS_SUB_SELECTIONS_MASK = 4;

      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      private static int makeHeaderByte(ColumnFilter selection) {
         return (selection.fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED?1:0) | (selection.queried != null?2:0) | (selection.subSelections != null?4:0);
      }

      public ColumnFilter maybeUpdateForBackwardCompatility(ColumnFilter selection) {
         if(((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.OSS_3014) <= 0 && selection.fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED && selection.queried != null) {
            List<ColumnMetadata> queriedStatic = new ArrayList();
            Iterables.addAll(queriedStatic, Iterables.filter(selection.queried, ColumnMetadata::isStatic));
            return new ColumnFilter(ColumnFilter.FetchType.COLUMNS_IN_QUERIED, (TableMetadata)null, new RegularAndStaticColumns(Columns.from(queriedStatic), selection.fetched.regulars), selection.subSelections);
         } else {
            return selection;
         }
      }

      public void serialize(ColumnFilter selection, DataOutputPlus out) throws IOException {
         selection = this.maybeUpdateForBackwardCompatility(selection);
         out.writeByte(makeHeaderByte(selection));
         if(((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.OSS_3014) >= 0 && selection.fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED) {
            Columns.serializer.serialize(selection.fetched.statics, out);
            Columns.serializer.serialize(selection.fetched.regulars, out);
         }

         if(selection.queried != null) {
            Columns.serializer.serialize(selection.queried.statics, out);
            Columns.serializer.serialize(selection.queried.regulars, out);
         }

         if(selection.subSelections != null) {
            out.writeUnsignedVInt((long)selection.subSelections.size());
            Iterator var3 = selection.subSelections.values().iterator();

            while(var3.hasNext()) {
               ColumnSubselection subSel = (ColumnSubselection)var3.next();
               ((ColumnSubselection.Serializer)ColumnSubselection.serializers.get(this.version)).serialize(subSel, out);
            }
         }

      }

      public ColumnFilter deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         int header = in.readUnsignedByte();
         boolean isFetchAll = (header & 1) != 0;
         boolean hasQueried = (header & 2) != 0;
         boolean hasSubSelections = (header & 4) != 0;
         RegularAndStaticColumns fetched = null;
         RegularAndStaticColumns queried = null;
         Columns statics;
         Columns regulars;
         if(isFetchAll) {
            if(((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.OSS_3014) >= 0) {
               statics = Columns.serializer.deserialize(in, metadata);
               regulars = Columns.serializer.deserialize(in, metadata);
               fetched = new RegularAndStaticColumns(statics, regulars);
            } else {
               fetched = metadata.regularAndStaticColumns();
            }
         }

         if(hasQueried) {
            statics = Columns.serializer.deserialize(in, metadata);
            regulars = Columns.serializer.deserialize(in, metadata);
            queried = new RegularAndStaticColumns(statics, regulars);
         }

         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections = null;
         if(hasSubSelections) {
            subSelections = TreeMultimap.create(Comparator.naturalOrder(), Comparator.naturalOrder());
            int size = (int)in.readUnsignedVInt();

            for(int i = 0; i < size; ++i) {
               ColumnSubselection subSel = ((ColumnSubselection.Serializer)ColumnSubselection.serializers.get(this.version)).deserialize(in, metadata);
               subSelections.put(subSel.column().name, subSel);
            }
         }

         if(this.version == ReadVerbs.ReadVersion.OSS_30 && isFetchAll && queried != null) {
            queried = new RegularAndStaticColumns(metadata.staticColumns(), queried.regulars);
         }

         ColumnFilter.FetchType fetchType = isFetchAll?ColumnFilter.FetchType.ALL_COLUMNS:ColumnFilter.FetchType.COLUMNS_IN_QUERIED;
         return (new ColumnFilter(fetchType, fetchType == ColumnFilter.FetchType.ALL_COLUMNS?fetched:queried, queried, subSelections)).withPartitionColumnsVerified(metadata.regularAndStaticColumns());
      }

      public long serializedSize(ColumnFilter selection) {
         selection = this.maybeUpdateForBackwardCompatility(selection);
         long size = 1L;
         if(((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.OSS_3014) >= 0 && selection.fetchType != ColumnFilter.FetchType.COLUMNS_IN_QUERIED) {
            size += Columns.serializer.serializedSize(selection.fetched.statics);
            size += Columns.serializer.serializedSize(selection.fetched.regulars);
         }

         if(selection.queried != null) {
            size += Columns.serializer.serializedSize(selection.queried.statics);
            size += Columns.serializer.serializedSize(selection.queried.regulars);
         }

         if(selection.subSelections != null) {
            size += (long)TypeSizes.sizeofUnsignedVInt((long)selection.subSelections.size());

            ColumnSubselection subSel;
            for(Iterator var4 = selection.subSelections.values().iterator(); var4.hasNext(); size += ((ColumnSubselection.Serializer)ColumnSubselection.serializers.get(this.version)).serializedSize(subSel)) {
               subSel = (ColumnSubselection)var4.next();
            }
         }

         return size;
      }
   }

   public static class Builder {
      private final TableMetadata metadata;
      private RegularAndStaticColumns.Builder queriedBuilder;
      private List<ColumnSubselection> subSelections;
      private Set<ColumnMetadata> fullySelectedComplexColumns;

      private Builder(TableMetadata metadata) {
         this.metadata = metadata;
      }

      public ColumnFilter.Builder add(ColumnMetadata c) {
         if(c.isComplex() && c.type.isMultiCell()) {
            if(this.fullySelectedComplexColumns == null) {
               this.fullySelectedComplexColumns = new HashSet();
            }

            this.fullySelectedComplexColumns.add(c);
         }

         return this.addInternal(c);
      }

      public ColumnFilter.Builder addAll(Iterable<ColumnMetadata> columns) {
         Iterator var2 = columns.iterator();

         while(var2.hasNext()) {
            ColumnMetadata column = (ColumnMetadata)var2.next();
            this.add(column);
         }

         return this;
      }

      private ColumnFilter.Builder addInternal(ColumnMetadata c) {
         if(c.isPrimaryKeyColumn()) {
            return this;
         } else {
            if(this.queriedBuilder == null) {
               this.queriedBuilder = RegularAndStaticColumns.builder();
            }

            this.queriedBuilder.add(c);
            return this;
         }
      }

      private ColumnFilter.Builder addSubSelection(ColumnSubselection subSelection) {
         ColumnMetadata column = subSelection.column();

         assert column.isComplex() && column.type.isMultiCell();

         this.addInternal(column);
         if(this.subSelections == null) {
            this.subSelections = new ArrayList();
         }

         this.subSelections.add(subSelection);
         return this;
      }

      public ColumnFilter.Builder slice(ColumnMetadata c, CellPath from, CellPath to) {
         return this.addSubSelection(ColumnSubselection.slice(c, from, to));
      }

      public ColumnFilter.Builder select(ColumnMetadata c, CellPath elt) {
         return this.addSubSelection(ColumnSubselection.element(c, elt));
      }

      public ColumnFilter build() {
         ColumnFilter.FetchType fetchType = this.metadata != null?ColumnFilter.FetchType.ALL_COLUMNS:ColumnFilter.FetchType.COLUMNS_IN_QUERIED;
         RegularAndStaticColumns queried = this.queriedBuilder == null?null:this.queriedBuilder.build();
         if(fetchType == ColumnFilter.FetchType.COLUMNS_IN_QUERIED && queried == null) {
            queried = RegularAndStaticColumns.NONE;
         }

         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = null;
         if(this.subSelections != null) {
            s = TreeMultimap.create(Comparator.naturalOrder(), Comparator.naturalOrder());
            Iterator var4 = this.subSelections.iterator();

            while(true) {
               ColumnSubselection subSelection;
               do {
                  if(!var4.hasNext()) {
                     return new ColumnFilter(fetchType, this.metadata, queried, s);
                  }

                  subSelection = (ColumnSubselection)var4.next();
               } while(this.fullySelectedComplexColumns != null && this.fullySelectedComplexColumns.contains(subSelection.column()));

               s.put(subSelection.column().name, subSelection);
            }
         } else {
            return new ColumnFilter(fetchType, this.metadata, queried, s);
         }
      }
   }

   public static class Tester {
      private final boolean isFetched;
      private ColumnSubselection current;
      private final Iterator<ColumnSubselection> iterator;

      private Tester(boolean isFetched, Iterator<ColumnSubselection> iterator) {
         this.isFetched = isFetched;
         this.iterator = iterator;
      }

      public boolean fetches(CellPath path) {
         return this.isFetched || this.hasSubselection(path);
      }

      public boolean fetchedCellIsQueried(CellPath path) {
         return !this.isFetched || this.hasSubselection(path);
      }

      private boolean hasSubselection(CellPath path) {
         while(this.current != null || this.iterator.hasNext()) {
            if(this.current == null) {
               this.current = (ColumnSubselection)this.iterator.next();
            }

            int cmp = this.current.compareInclusionOf(path);
            if(cmp == 0) {
               return true;
            }

            if(cmp < 0) {
               return false;
            }

            this.current = null;
         }

         return false;
      }
   }

   static enum FetchType {
      ALL_COLUMNS,
      COLUMNS_IN_FETCHED,
      COLUMNS_IN_QUERIED;

      private FetchType() {
      }
   }
}
