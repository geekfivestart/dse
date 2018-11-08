package org.apache.cassandra.index.sasi.plan;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.TermIterator;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.conf.view.View;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.exceptions.TimeQuotaExceededException;
import org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.Flow;

public class QueryController {
   private final long executionQuota;
   private final long executionStart;
   private final ColumnFamilyStore cfs;
   private final PartitionRangeReadCommand command;
   private final DataRange range;
   private final Map<Collection<Expression>, List<RangeIterator<Long, Token>>> resources = new HashMap();

   public QueryController(ColumnFamilyStore cfs, PartitionRangeReadCommand command, long timeQuotaMs) {
      this.cfs = cfs;
      this.command = command;
      this.range = command.dataRange();
      this.executionQuota = TimeUnit.MILLISECONDS.toNanos(timeQuotaMs);
      this.executionStart = System.nanoTime();
   }

   public TableMetadata metadata() {
      return this.command.metadata();
   }

   public Collection<RowFilter.Expression> getExpressions() {
      return this.command.rowFilter().getExpressions();
   }

   public DataRange dataRange() {
      return this.command.dataRange();
   }

   public AbstractType<?> getKeyValidator() {
      return this.cfs.metadata().partitionKeyType;
   }

   public ColumnIndex getIndex(RowFilter.Expression expression) {
      Optional<Index> index = this.cfs.indexManager.getBestIndexFor(expression);
      return index.isPresent()?((SASIIndex)index.get()).getIndex():null;
   }

   public Flow<FlowableUnfilteredPartition> getPartition(DecoratedKey key, ReadExecutionController executionController) {
      if(key == null) {
         throw new NullPointerException();
      } else {
         Flow var4;
         try {
            SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(this.cfs.metadata(), this.command.nowInSec(), this.command.columnFilter(), this.command.rowFilter().withoutExpressions(), DataLimits.NONE, key, this.command.clusteringIndexFilter(key));
            var4 = partition.queryStorage(this.cfs, executionController);
         } finally {
            this.checkpoint();
         }

         return var4;
      }
   }

   public RangeIterator.Builder<Long, Token> getIndexes(Operation.OperationType op, Collection<Expression> expressions) {
      if(this.resources.containsKey(expressions)) {
         throw new IllegalArgumentException("Can't process the same expressions multiple times.");
      } else {
         RangeIterator.Builder<Long, Token> builder = op == Operation.OperationType.OR?RangeUnionIterator.builder():RangeIntersectionIterator.builder();
         Set<Entry<Expression, Set<SSTableIndex>>> view = this.getView(op, expressions).entrySet();
         List<RangeIterator<Long, Token>> perIndexUnions = new ArrayList(view.size());
         Iterator var6 = view.iterator();

         while(var6.hasNext()) {
            Entry<Expression, Set<SSTableIndex>> e = (Entry)var6.next();
            RangeIterator<Long, Token> index = TermIterator.build((Expression)e.getKey(), (Set)e.getValue());
            ((RangeIterator.Builder)builder).add((RangeIterator)index);
            perIndexUnions.add(index);
         }

         this.resources.put(expressions, perIndexUnions);
         return (RangeIterator.Builder)builder;
      }
   }

   public void checkpoint() {
      long executionTime = System.nanoTime() - this.executionStart;
      if(executionTime >= this.executionQuota) {
         throw new TimeQuotaExceededException("Command '" + this.command + "' took too long (" + TimeUnit.NANOSECONDS.toMillis(executionTime) + " >= " + TimeUnit.NANOSECONDS.toMillis(this.executionQuota) + "ms).");
      }
   }

   public void releaseIndexes(Operation operation) {
      if(operation.expressions != null) {
         this.releaseIndexes((List)this.resources.remove(operation.expressions.values()));
      }

   }

   private void releaseIndexes(List<RangeIterator<Long, Token>> indexes) {
      if(indexes != null) {
         indexes.forEach(FileUtils::closeQuietly);
      }
   }

   public void finish() {
      this.resources.values().forEach(this::releaseIndexes);
   }

   private Map<Expression, Set<SSTableIndex>> getView(Operation.OperationType op, Collection<Expression> expressions) {
      Pair<Expression, Set<SSTableIndex>> primary = op == Operation.OperationType.AND?this.calculatePrimary(expressions):null;
      Map<Expression, Set<SSTableIndex>> indexes = new HashMap();
      Iterator var5 = expressions.iterator();

      while(true) {
         while(true) {
            Expression e;
            do {
               do {
                  if(!var5.hasNext()) {
                     return indexes;
                  }

                  e = (Expression)var5.next();
               } while(!e.isIndexed());
            } while(e.getOp() == Expression.Op.NOT_EQ);

            if(primary != null && e.equals(primary.left)) {
               indexes.put(primary.left, primary.right);
            } else {
               View view = e.index.getView();
               if(view != null) {
                  Set<SSTableIndex> readers = new HashSet();
                  if(primary != null && ((Set)primary.right).size() > 0) {
                     Iterator var9 = ((Set)primary.right).iterator();

                     while(var9.hasNext()) {
                        SSTableIndex index = (SSTableIndex)var9.next();
                        readers.addAll(view.match(index.minKey(), index.maxKey()));
                     }
                  } else {
                     readers.addAll(this.applyScope(view.match(e)));
                  }

                  indexes.put(e, readers);
               }
            }
         }
      }
   }

   private Pair<Expression, Set<SSTableIndex>> calculatePrimary(Collection<Expression> expressions) {
      Expression expression = null;
      Set<SSTableIndex> primaryIndexes = Collections.emptySet();
      Iterator var4 = expressions.iterator();

      while(true) {
         Expression e;
         Set indexes;
         do {
            View view;
            do {
               do {
                  if(!var4.hasNext()) {
                     return expression == null?null:Pair.create(expression, primaryIndexes);
                  }

                  e = (Expression)var4.next();
               } while(!e.isIndexed());

               view = e.index.getView();
            } while(view == null);

            indexes = this.applyScope(view.match(e));
         } while(expression != null && primaryIndexes.size() <= indexes.size());

         primaryIndexes = indexes;
         expression = e;
      }
   }

   private Set<SSTableIndex> applyScope(Set<SSTableIndex> indexes) {
      return Sets.filter(indexes, (index) -> {
         SSTableReader sstable = index.getSSTable();
         return this.range.startKey().compareTo(sstable.last) <= 0 && (this.range.stopKey().isMinimum() || sstable.first.compareTo(this.range.stopKey()) <= 0);
      });
   }
}
