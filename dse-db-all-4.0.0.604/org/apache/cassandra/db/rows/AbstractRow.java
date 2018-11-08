package org.apache.cassandra.db.rows;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

public abstract class AbstractRow extends AbstractCollection<ColumnData> implements Row {
   protected final Clustering clustering;
   protected final LivenessInfo primaryKeyLivenessInfo;
   protected final Row.Deletion deletion;
   protected Collection<ColumnMetadata> columns;
   protected final int minLocalDeletionTime;

   protected AbstractRow(Clustering clustering, LivenessInfo primaryKeyLivenessInfo, Row.Deletion deletion, int minLocalDeletionTime) {
      assert !deletion.isShadowedBy(primaryKeyLivenessInfo);

      this.clustering = clustering;
      this.primaryKeyLivenessInfo = primaryKeyLivenessInfo;
      this.deletion = deletion;
      this.minLocalDeletionTime = minLocalDeletionTime;
   }

   public abstract void setValue(ColumnMetadata var1, CellPath var2, ByteBuffer var3);

   protected static int minDeletionTime(Cell cell) {
      return cell.isTombstone()?-2147483648:cell.localDeletionTime();
   }

   protected static int minDeletionTime(LivenessInfo info) {
      return info.isExpiring()?info.localExpirationTime():2147483647;
   }

   protected static int minDeletionTime(DeletionTime dt) {
      return dt.isLive()?2147483647:-2147483648;
   }

   protected static int minDeletionTime(ComplexColumnData cd) {
      int min = minDeletionTime(cd.complexDeletion());
      Iterator var2 = cd.iterator();

      while(var2.hasNext()) {
         Cell cell = (Cell)var2.next();
         min = Math.min(min, minDeletionTime(cell));
         if(min == -2147483648) {
            break;
         }
      }

      return min;
   }

   protected static int minDeletionTime(ColumnData cd) {
      return cd.column().isSimple()?minDeletionTime((Cell)cd):minDeletionTime((ComplexColumnData)cd);
   }

   public Clustering clustering() {
      return this.clustering;
   }

   public Collection<ColumnMetadata> columns() {
      if(this.columns == null) {
         this.columns = (Collection)this.reduce(new ArrayList(this.size()), (list, c) -> {
            list.add(c.column);
            return list;
         });
      }

      return this.columns;
   }

   public Row.Deletion deletion() {
      return this.deletion;
   }

   public LivenessInfo primaryKeyLivenessInfo() {
      return this.primaryKeyLivenessInfo;
   }

   public Unfiltered.Kind kind() {
      return Unfiltered.Kind.ROW;
   }

   public boolean hasDeletion(int nowInSec) {
      return nowInSec >= this.minLocalDeletionTime;
   }

   public Row filter(ColumnFilter filter, TableMetadata metadata) {
      return this.filter(filter, DeletionTime.LIVE, false, metadata);
   }

   public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata) {
      Map<ByteBuffer, DroppedColumn> droppedColumns = metadata.droppedColumns;
      boolean mayFilterColumns = !filter.fetchesAllColumns(this.isStatic()) || !filter.allFetchedColumnsAreQueried();
      boolean mayHaveShadowed = activeDeletion.supersedes(this.deletion.time());
      if(!mayFilterColumns && !mayHaveShadowed && droppedColumns.isEmpty()) {
         return this;
      } else {
         LivenessInfo newInfo = this.primaryKeyLivenessInfo;
         Row.Deletion newDeletion = this.deletion;
         if(mayHaveShadowed) {
            if(activeDeletion.deletes(newInfo.timestamp())) {
               newInfo = LivenessInfo.EMPTY;
            }

            newDeletion = setActiveDeletionToRow?Row.Deletion.regular(activeDeletion):Row.Deletion.LIVE;
         }

         Columns columns = filter.fetchedColumns().columns(this.isStatic());
         Predicate<ColumnMetadata> inclusionTester = columns.inOrderInclusionTester();
         Predicate<ColumnMetadata> queriedByUserTester = filter.queriedColumns().columns(this.isStatic()).inOrderInclusionTester();
         return this.transformAndFilter(newInfo, newDeletion, (cd) -> {
            ColumnMetadata column = cd.column();
            if(!inclusionTester.test(column)) {
               return null;
            } else {
               DroppedColumn dropped = (DroppedColumn)droppedColumns.get(column.name.bytes);
               if(column.isComplex()) {
                  return ((ComplexColumnData)cd).filter(filter, mayHaveShadowed?activeDeletion:DeletionTime.LIVE, dropped, newInfo);
               } else {
                  Cell cell = (Cell)cd;
                  boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
                  boolean isShadowed = mayHaveShadowed && activeDeletion.deletes(cell);
                  boolean isSkippable = !queriedByUserTester.test(column) && cell.timestamp() < newInfo.timestamp();
                  if(!isForDropped && !isShadowed && !isSkippable) {
                     boolean shouldSkipValue = !filter.fetchedColumnIsQueried(column);
                     return shouldSkipValue?cell.withSkippedValue():cell;
                  } else {
                     return null;
                  }
               }
            }
         });
      }
   }

   abstract Row transformAndFilter(LivenessInfo var1, Row.Deletion var2, Function<ColumnData, ColumnData> var3);

   public Row purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness) {
      if(!this.hasDeletion(nowInSec)) {
         return this;
      } else {
         LivenessInfo newInfo = purger.shouldPurge(this.primaryKeyLivenessInfo, nowInSec)?LivenessInfo.EMPTY:this.primaryKeyLivenessInfo;
         Row.Deletion newDeletion = purger.shouldPurge(this.deletion.time())?Row.Deletion.LIVE:this.deletion;
         return enforceStrictLiveness && newDeletion.isLive() && newInfo.isEmpty()?null:this.transformAndFilter(newInfo, newDeletion, (cd) -> {
            return cd.purge(purger, nowInSec);
         });
      }
   }

   public Row withOnlyQueriedData(ColumnFilter filter) {
      return (Row)(filter.allFetchedColumnsAreQueried()?this:this.transformAndFilter(this.primaryKeyLivenessInfo, this.deletion, (cd) -> {
         ColumnMetadata column = cd.column();
         return (ColumnData)(column.isComplex()?((ComplexColumnData)cd).withOnlyQueriedData(filter):(filter.fetchedColumnIsQueried(column)?cd:null));
      }));
   }

   public Row markCounterLocalToBeCleared() {
      return this.transformAndFilter(this.primaryKeyLivenessInfo, this.deletion, (cd) -> {
         return cd.column().isCounterColumn()?cd.markCounterLocalToBeCleared():cd;
      });
   }

   public Row updateAllTimestamp(long newTimestamp) {
      LivenessInfo newInfo = this.primaryKeyLivenessInfo.isEmpty()?this.primaryKeyLivenessInfo:this.primaryKeyLivenessInfo.withUpdatedTimestamp(newTimestamp);
      Row.Deletion newDeletion = !this.deletion.isLive() && (!this.deletion.isShadowable() || this.primaryKeyLivenessInfo.isEmpty())?new Row.Deletion(new DeletionTime(newTimestamp - 1L, this.deletion.time().localDeletionTime()), this.deletion.isShadowable()):Row.Deletion.LIVE;
      return this.transformAndFilter(newInfo, newDeletion, (cd) -> {
         return cd.updateAllTimestamp(newTimestamp);
      });
   }

   public boolean hasLiveData(final int nowInSec, boolean enforceStrictLiveness) {
      return this.primaryKeyLivenessInfo().isLive(nowInSec)?true:(enforceStrictLiveness?false:((Boolean)this.reduceCells(Boolean.valueOf(false), new BTree.ReduceFunction<Boolean, Cell>() {
         public Boolean apply(Boolean ret, Cell cell) {
            return Boolean.valueOf(ret.booleanValue() || cell.isLive(nowInSec));
         }

         public boolean stop(Boolean ret) {
            return ret.booleanValue();
         }
      })).booleanValue());
   }

   public boolean isStatic() {
      return this.clustering() == Clustering.STATIC_CLUSTERING;
   }

   public void digest(Hasher hasher) {
      HashingUtils.updateWithByte(hasher, this.kind().ordinal());
      this.clustering().digest(hasher);
      this.deletion().digest(hasher);
      this.primaryKeyLivenessInfo().digest(hasher);
      Iterator var2 = this.iterator();

      while(var2.hasNext()) {
         ColumnData cd = (ColumnData)var2.next();
         cd.digest(hasher);
      }

   }

   public void validateData(TableMetadata metadata) {
      Clustering clustering = this.clustering();

      for(int i = 0; i < clustering.size(); ++i) {
         ByteBuffer value = clustering.get(i);
         if(value != null) {
            metadata.comparator.subtype(i).validate(value);
         }
      }

      this.primaryKeyLivenessInfo().validate();
      if(this.deletion().time().localDeletionTime() < 0) {
         throw new MarshalException("A local deletion time should not be negative");
      } else {
         Iterator var5 = this.iterator();

         while(var5.hasNext()) {
            ColumnData cd = (ColumnData)var5.next();
            cd.validate();
         }

      }
   }

   public String toString(TableMetadata metadata) {
      return this.toString(metadata, false);
   }

   public String toString(TableMetadata metadata, boolean fullDetails) {
      return this.toString(metadata, true, fullDetails);
   }

   public String toString(TableMetadata metadata, boolean includeClusterKeys, boolean fullDetails) {
      StringBuilder sb = new StringBuilder();
      sb.append("Row");
      if(fullDetails) {
         sb.append("[info=").append(this.primaryKeyLivenessInfo());
         if(!this.deletion().isLive()) {
            sb.append(" del=").append(this.deletion());
         }

         sb.append(" ]");
      }

      sb.append(": ");
      if(includeClusterKeys) {
         sb.append(this.clustering().toString(metadata));
      } else {
         sb.append(this.clustering().toCQLString(metadata));
      }

      sb.append(" | ");
      boolean isFirst = true;
      Iterator var6 = this.iterator();

      while(true) {
         while(true) {
            while(var6.hasNext()) {
               ColumnData cd = (ColumnData)var6.next();
               if(isFirst) {
                  isFirst = false;
               } else {
                  sb.append(", ");
               }

               ComplexColumnData complexData;
               if(fullDetails) {
                  if(cd.column().isSimple()) {
                     sb.append(cd);
                  } else {
                     complexData = (ComplexColumnData)cd;
                     if(!complexData.complexDeletion().isLive()) {
                        sb.append("del(").append(cd.column().name).append(")=").append(complexData.complexDeletion());
                     }

                     Iterator var12 = complexData.iterator();

                     while(var12.hasNext()) {
                        Cell cell = (Cell)var12.next();
                        sb.append(", ").append(cell);
                     }
                  }
               } else if(cd.column().isSimple()) {
                  Cell cell = (Cell)cd;
                  sb.append(cell.column().name).append('=');
                  if(cell.isTombstone()) {
                     sb.append("<tombstone>");
                  } else {
                     sb.append(cell.column().type.getString(cell.value()));
                  }
               } else {
                  sb.append(cd.column().name).append('=');
                  complexData = (ComplexColumnData)cd;
                  java.util.function.Function transform;
                  if(cd.column().type.isCollection()) {
                     CollectionType ct = (CollectionType)cd.column().type;
                     transform = (cell) -> {
                        return String.format("%s -> %s", new Object[]{ct.nameComparator().getString(cell.path().get(0)), ct.valueComparator().getString(cell.value())});
                     };
                  } else if(cd.column().type.isUDT()) {
                     UserType ut = (UserType)cd.column().type;
                     transform = (cell) -> {
                        Short fId = (Short)ut.nameComparator().getSerializer().deserialize(cell.path().get(0));
                        return String.format("%s -> %s", new Object[]{ut.fieldNameAsString(fId.shortValue()), ut.fieldType(fId.shortValue()).getString(cell.value())});
                     };
                  } else {
                     transform = (cell) -> {
                        return "";
                     };
                  }

                  sb.append((String)StreamSupport.stream(complexData.spliterator(), false).map(transform).collect(Collectors.joining(", ", "{", "}")));
               }
            }

            return sb.toString();
         }
      }
   }

   public boolean equals(Object other) {
      if(!(other instanceof Row)) {
         return false;
      } else {
         Row that = (Row)other;
         return this.clustering().equals(that.clustering()) && this.primaryKeyLivenessInfo().equals(that.primaryKeyLivenessInfo()) && this.deletion().equals(that.deletion())?Iterables.elementsEqual(this, that):false;
      }
   }

   public int hashCode() {
      int hash = Objects.hash(new Object[]{this.clustering(), this.primaryKeyLivenessInfo(), this.deletion()});

      ColumnData cd;
      for(Iterator var2 = this.iterator(); var2.hasNext(); hash += 31 * cd.hashCode()) {
         cd = (ColumnData)var2.next();
      }

      return hash;
   }

   static class CellResolver implements BTree.Builder.Resolver {
      final int nowInSec;

      CellResolver(int nowInSec) {
         this.nowInSec = nowInSec;
      }

      public ColumnData resolve(Object[] cells, int lb, int ub) {
         Cell cell = (Cell)cells[lb];
         ColumnMetadata column = cell.column;
         if(cell.column.isSimple()) {
            assert lb + 1 == ub || this.nowInSec != -2147483648;

            while(true) {
               ++lb;
               if(lb >= ub) {
                  return cell;
               }

               cell = Cells.reconcile(cell, (Cell)cells[lb], this.nowInSec);
            }
         } else {
            Arrays.sort(cells, lb, ub, column.cellComparator());

            DeletionTime deletion;
            for(deletion = DeletionTime.LIVE; lb < ub; ++lb) {
               cell = (Cell)cells[lb];
               if(!(cell instanceof AbstractRow.ComplexColumnDeletion)) {
                  break;
               }

               if(cell.timestamp() > deletion.markedForDeleteAt()) {
                  deletion = new DeletionTime(cell.timestamp(), cell.localDeletionTime());
               }
            }

            List<Object> buildFrom = new ArrayList(ub - lb);
            Cell previous = null;

            for(int i = lb; i < ub; ++i) {
               Cell c = (Cell)cells[i];
               if(deletion == DeletionTime.LIVE || c.timestamp() >= deletion.markedForDeleteAt()) {
                  if(previous != null && column.cellComparator().compare(previous, c) == 0) {
                     c = Cells.reconcile(previous, c, this.nowInSec);
                     buildFrom.set(buildFrom.size() - 1, c);
                  } else {
                     buildFrom.add(c);
                  }

                  previous = c;
               }
            }

            Object[] btree = BTree.build((Collection)buildFrom, UpdateFunction.noOp());
            return new ComplexColumnData(column, btree, deletion);
         }
      }
   }

   protected static class ComplexColumnDeletion extends BufferCell {
      public ComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime) {
         super(column, deletionTime.markedForDeleteAt(), 0, deletionTime.localDeletionTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.BOTTOM);
      }
   }
}
