package org.apache.cassandra.index.sasi.conf;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.view.View;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.memory.IndexMemtable;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnIndex {
   private static final String FILE_NAME_FORMAT = "SI_%s.db";
   private final AbstractType<?> keyValidator;
   private final ColumnMetadata column;
   private final Optional<IndexMetadata> config;
   private final AtomicReference<IndexMemtable> memtable;
   private final ConcurrentMap<Memtable, IndexMemtable> pendingFlush = new ConcurrentHashMap();
   private final IndexMode mode;
   private final Component component;
   private final DataTracker tracker;
   private final boolean isTokenized;

   public ColumnIndex(AbstractType<?> keyValidator, ColumnMetadata column, IndexMetadata metadata) {
      this.keyValidator = keyValidator;
      this.column = column;
      this.config = metadata == null?Optional.empty():Optional.of(metadata);
      this.mode = IndexMode.getMode(column, this.config);
      this.memtable = new AtomicReference(new IndexMemtable(this));
      this.tracker = new DataTracker(keyValidator, this);
      this.component = new Component(Component.Type.SECONDARY_INDEX, String.format("SI_%s.db", new Object[]{this.getIndexName()}));
      this.isTokenized = this.getAnalyzer().isTokenizing();
   }

   public Iterable<SSTableReader> init(Set<SSTableReader> sstables) {
      return this.tracker.update(Collections.emptySet(), sstables);
   }

   public AbstractType<?> keyValidator() {
      return this.keyValidator;
   }

   public long index(DecoratedKey key, Row row) {
      return this.getCurrentMemtable().index(key, getValueOf(this.column, row, FBUtilities.nowInSeconds()));
   }

   public void switchMemtable() {
      this.memtable.set(new IndexMemtable(this));
   }

   public void switchMemtable(Memtable parent) {
      this.pendingFlush.putIfAbsent(parent, this.memtable.getAndSet(new IndexMemtable(this)));
   }

   public void discardMemtable(Memtable parent) {
      this.pendingFlush.remove(parent);
   }

   @VisibleForTesting
   public IndexMemtable getCurrentMemtable() {
      return (IndexMemtable)this.memtable.get();
   }

   @VisibleForTesting
   public Collection<IndexMemtable> getPendingMemtables() {
      return this.pendingFlush.values();
   }

   public RangeIterator<Long, Token> searchMemtable(Expression e) {
      RangeIterator.Builder<Long, Token> builder = new RangeUnionIterator.Builder();
      builder.add(this.getCurrentMemtable().search(e));
      Iterator var3 = this.getPendingMemtables().iterator();

      while(var3.hasNext()) {
         IndexMemtable memtable = (IndexMemtable)var3.next();
         builder.add(memtable.search(e));
      }

      return builder.build();
   }

   public void update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables) {
      this.tracker.update(oldSSTables, newSSTables);
   }

   public ColumnMetadata getDefinition() {
      return this.column;
   }

   public AbstractType<?> getValidator() {
      return this.column.cellValueType();
   }

   public Component getComponent() {
      return this.component;
   }

   public IndexMode getMode() {
      return this.mode;
   }

   public String getColumnName() {
      return this.column.name.toString();
   }

   public String getIndexName() {
      return this.config.isPresent()?((IndexMetadata)this.config.get()).name:"undefined";
   }

   public AbstractAnalyzer getAnalyzer() {
      AbstractAnalyzer analyzer = this.mode.getAnalyzer(this.getValidator());
      analyzer.init(this.config.isPresent()?((IndexMetadata)this.config.get()).options:Collections.emptyMap(), this.column.cellValueType());
      return analyzer;
   }

   public View getView() {
      return this.tracker.getView();
   }

   public boolean hasSSTable(SSTableReader sstable) {
      return this.tracker.hasSSTable(sstable);
   }

   public void dropData(Collection<SSTableReader> sstablesToRebuild) {
      this.tracker.dropData(sstablesToRebuild);
   }

   public void dropData(long truncateUntil) {
      this.switchMemtable();
      this.tracker.dropData(truncateUntil);
   }

   public boolean isIndexed() {
      return this.mode != IndexMode.NOT_INDEXED;
   }

   public boolean isLiteral() {
      AbstractType<?> validator = this.getValidator();
      return this.isIndexed()?this.mode.isLiteral:validator instanceof UTF8Type || validator instanceof AsciiType;
   }

   public boolean supports(Operator op) {
      if(op == Operator.LIKE) {
         return this.isLiteral();
      } else {
         Expression.Op operator = Expression.Op.valueOf(op);
         return (!this.isTokenized || operator != Expression.Op.EQ) && (!this.isTokenized || this.mode.mode != OnDiskIndexBuilder.Mode.CONTAINS || operator != Expression.Op.PREFIX) && (!this.isLiteral() || operator != Expression.Op.RANGE) && this.mode.supports(operator);
      }
   }

   public static ByteBuffer getValueOf(ColumnMetadata column, Row row, int nowInSecs) {
      if(row == null) {
         return null;
      } else {
         switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[column.kind.ordinal()]) {
         case 1:
            if(row.isStatic()) {
               return null;
            }

            return row.clustering().get(column.position());
         case 2:
            if(!row.isStatic()) {
               return null;
            }
         case 3:
            break;
         default:
            return null;
         }

         Cell cell = row.getCell(column);
         return cell != null && cell.isLive(nowInSecs)?cell.value():null;
      }
   }

   public String toString() {
      return "ColumnIndex(column = " + this.getColumnName() + ", name = " + this.getIndexName() + ")";
   }

   public boolean equals(Object obj) {
      if(obj == this) {
         return true;
      } else if(!(obj instanceof ColumnIndex)) {
         return false;
      } else {
         ColumnIndex other = (ColumnIndex)obj;
         return Objects.equals(this.column, other.column) && Objects.equals(this.config, other.config) && Objects.equals(this.keyValidator, other.keyValidator);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.column, this.config, this.keyValidator});
   }
}
