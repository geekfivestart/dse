package org.apache.cassandra.cql3.restrictions;

import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;

public final class RestrictionSet implements Restrictions, Iterable<SingleRestriction> {
   private static final Comparator<ColumnMetadata> COLUMN_DEFINITION_COMPARATOR = new Comparator<ColumnMetadata>() {
      public int compare(ColumnMetadata column, ColumnMetadata otherColumn) {
         int value = Integer.compare(column.position(), otherColumn.position());
         return value != 0?value:column.name.bytes.compareTo(otherColumn.name.bytes);
      }
   };
   protected final TreeMap<ColumnMetadata, SingleRestriction> restrictions;
   private final boolean hasMultiColumnRestrictions;
   private final boolean hasIn;

   public RestrictionSet() {
      this(new TreeMap(COLUMN_DEFINITION_COMPARATOR), false, false);
   }

   private RestrictionSet(TreeMap<ColumnMetadata, SingleRestriction> restrictions, boolean hasMultiColumnRestrictions, boolean hasIN) {
      this.restrictions = restrictions;
      this.hasMultiColumnRestrictions = hasMultiColumnRestrictions;
      this.hasIn = hasIN;
   }

   public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException {
      Iterator var4 = this.restrictions.values().iterator();

      while(var4.hasNext()) {
         Restriction restriction = (Restriction)var4.next();
         restriction.addRowFilterTo(filter, indexManager, options);
      }

   }

   public List<ColumnMetadata> getColumnDefs() {
      return new ArrayList(this.restrictions.keySet());
   }

   public void addFunctionsTo(List<Function> functions) {
      Iterator var2 = this.iterator();

      while(var2.hasNext()) {
         Restriction restriction = (Restriction)var2.next();
         restriction.addFunctionsTo(functions);
      }

   }

   public boolean isEmpty() {
      return this.restrictions.isEmpty();
   }

   public int size() {
      return this.restrictions.size();
   }

   public boolean hasRestrictionFor(ColumnMetadata.Kind kind) {
      Iterator var2 = this.restrictions.keySet().iterator();

      ColumnMetadata column;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         column = (ColumnMetadata)var2.next();
      } while(column.kind != kind);

      return true;
   }

   public RestrictionSet addRestriction(SingleRestriction restriction) {
      TreeMap<ColumnMetadata, SingleRestriction> newRestrictions = new TreeMap(this.restrictions);
      return new RestrictionSet(this.mergeRestrictions(newRestrictions, restriction), this.hasMultiColumnRestrictions || restriction.isMultiColumn(), this.hasIn || restriction.isIN());
   }

   private TreeMap<ColumnMetadata, SingleRestriction> mergeRestrictions(TreeMap<ColumnMetadata, SingleRestriction> restrictions, SingleRestriction restriction) {
      Collection<ColumnMetadata> columnDefs = restriction.getColumnDefs();
      Set<SingleRestriction> existingRestrictions = this.getRestrictions((Collection)columnDefs);
      Iterator var5;
      if(existingRestrictions.isEmpty()) {
         var5 = columnDefs.iterator();

         while(var5.hasNext()) {
            ColumnMetadata columnDef = (ColumnMetadata)var5.next();
            restrictions.put(columnDef, restriction);
         }
      } else {
         var5 = existingRestrictions.iterator();

         while(var5.hasNext()) {
            SingleRestriction existing = (SingleRestriction)var5.next();
            SingleRestriction newRestriction = mergeRestrictions(existing, restriction);
            Iterator var8 = columnDefs.iterator();

            while(var8.hasNext()) {
               ColumnMetadata columnDef = (ColumnMetadata)var8.next();
               restrictions.put(columnDef, newRestriction);
            }
         }
      }

      return restrictions;
   }

   public Set<Restriction> getRestrictions(ColumnMetadata columnDef) {
      Restriction existing = (Restriction)this.restrictions.get(columnDef);
      return existing == null?Collections.emptySet():Collections.singleton(existing);
   }

   private Set<SingleRestriction> getRestrictions(Collection<ColumnMetadata> columnDefs) {
      Set<SingleRestriction> set = new HashSet();
      Iterator var3 = columnDefs.iterator();

      while(var3.hasNext()) {
         ColumnMetadata columnDef = (ColumnMetadata)var3.next();
         SingleRestriction existing = (SingleRestriction)this.restrictions.get(columnDef);
         if(existing != null) {
            set.add(existing);
         }
      }

      return set;
   }

   public final boolean hasSupportingIndex(SecondaryIndexManager indexManager) {
      Iterator var2 = this.restrictions.values().iterator();

      Restriction restriction;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         restriction = (Restriction)var2.next();
      } while(!restriction.hasSupportingIndex(indexManager));

      return true;
   }

   ColumnMetadata nextColumn(ColumnMetadata columnDef) {
      return (ColumnMetadata)this.restrictions.tailMap(columnDef, false).firstKey();
   }

   public ColumnMetadata getFirstColumn() {
      return this.isEmpty()?null:(ColumnMetadata)this.restrictions.firstKey();
   }

   public ColumnMetadata getLastColumn() {
      return this.isEmpty()?null:(ColumnMetadata)this.restrictions.lastKey();
   }

   SingleRestriction lastRestriction() {
      return this.isEmpty()?null:(SingleRestriction)this.restrictions.lastEntry().getValue();
   }

   private static SingleRestriction mergeRestrictions(SingleRestriction restriction, SingleRestriction otherRestriction) {
      return restriction == null?otherRestriction:restriction.mergeWith(otherRestriction);
   }

   public final boolean hasMultipleContains() {
      int numberOfContains = 0;
      Iterator var2 = this.restrictions.values().iterator();

      while(var2.hasNext()) {
         SingleRestriction restriction = (SingleRestriction)var2.next();
         if(restriction.isContains()) {
            SingleColumnRestriction.ContainsRestriction contains = (SingleColumnRestriction.ContainsRestriction)restriction;
            numberOfContains += contains.numberOfValues() + contains.numberOfKeys() + contains.numberOfEntries();
         }
      }

      return numberOfContains > 1;
   }

   public Iterator<SingleRestriction> iterator() {
      Iterator<SingleRestriction> iterator = this.restrictions.values().iterator();
      return (Iterator)(this.hasMultiColumnRestrictions?new RestrictionSet.DistinctIterator(iterator):iterator);
   }

   public final boolean hasIN() {
      return this.hasIn;
   }

   public boolean hasContains() {
      Iterator var1 = this.iterator();

      SingleRestriction restriction;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         restriction = (SingleRestriction)var1.next();
      } while(!restriction.isContains());

      return true;
   }

   public final boolean hasSlice() {
      Iterator var1 = this.iterator();

      SingleRestriction restriction;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         restriction = (SingleRestriction)var1.next();
      } while(!restriction.isSlice());

      return true;
   }

   public final boolean hasOnlyEqualityRestrictions() {
      Iterator var1 = this.iterator();

      SingleRestriction restriction;
      do {
         if(!var1.hasNext()) {
            return true;
         }

         restriction = (SingleRestriction)var1.next();
      } while(restriction.isEQ() || restriction.isIN());

      return false;
   }

   private static final class DistinctIterator<E> extends AbstractIterator<E> {
      private final Iterator<E> iterator;
      private E previous;

      public DistinctIterator(Iterator<E> iterator) {
         this.iterator = iterator;
      }

      protected E computeNext() {
         while(true) {
            if(this.iterator.hasNext()) {
               E next = this.iterator.next();
               if(next.equals(this.previous)) {
                  continue;
               }

               this.previous = next;
               return next;
            }

            return this.endOfData();
         }
      }
   }
}
