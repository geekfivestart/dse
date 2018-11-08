package org.apache.cassandra.cql3;

import com.google.common.collect.UnmodifiableIterator;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscription;

public abstract class UntypedResultSet implements Iterable<UntypedResultSet.Row> {
   public static final UntypedResultSet EMPTY = create(Collections.emptyList());

   public UntypedResultSet() {
   }

   public static UntypedResultSet create(ResultSet rs) {
      return new UntypedResultSet.FromResultSet(rs);
   }

   public static UntypedResultSet create(List<Map<String, ByteBuffer>> results) {
      return new UntypedResultSet.FromResultList(results);
   }

   public static UntypedResultSet create(SelectStatement select, QueryPager pager, PageSize pageSize) {
      return create(select, pager, pageSize, (ReadContext)null);
   }

   public static UntypedResultSet create(SelectStatement select, QueryPager pager, PageSize pageSize, ReadContext params) {
      return new UntypedResultSet.FromPager(select, pager, pageSize, params);
   }

   public boolean isEmpty() {
      return this.size() == 0;
   }

   public abstract int size();

   public abstract UntypedResultSet.Row one();

   public abstract Flow<UntypedResultSet.Row> rows();

   public abstract List<ColumnSpecification> metadata();

   public static class Row {
      private final Map<String, ByteBuffer> data = new HashMap();
      private final List<ColumnSpecification> columns = new ArrayList();

      public Row(Map<String, ByteBuffer> data) {
         this.data.putAll(data);
      }

      public Row(List<ColumnSpecification> names, List<ByteBuffer> columns) {
         this.columns.addAll(names);

         for(int i = 0; i < names.size(); ++i) {
            this.data.put(((ColumnSpecification)names.get(i)).name.toString(), columns.get(i));
         }

      }

      public static UntypedResultSet.Row fromInternalRow(TableMetadata metadata, DecoratedKey key, org.apache.cassandra.db.rows.Row row) {
         Map<String, ByteBuffer> data = new HashMap();
         ByteBuffer[] keyComponents = SelectStatement.getComponents(metadata, key);
         UnmodifiableIterator var5 = metadata.partitionKeyColumns().iterator();

         while(var5.hasNext()) {
            ColumnMetadata def = (ColumnMetadata)var5.next();
            data.put(def.name.toString(), keyComponents[def.position()]);
         }

         Clustering clustering = row.clustering();
         UnmodifiableIterator var10 = metadata.clusteringColumns().iterator();

         ColumnMetadata def;
         while(var10.hasNext()) {
            def = (ColumnMetadata)var10.next();
            data.put(def.name.toString(), clustering.get(def.position()));
         }

         Iterator var11 = metadata.regularAndStaticColumns().iterator();

         while(var11.hasNext()) {
            def = (ColumnMetadata)var11.next();
            if(def.isSimple()) {
               Cell cell = row.getCell(def);
               if(cell != null) {
                  data.put(def.name.toString(), cell.value());
               }
            } else {
               ComplexColumnData complexData = row.getComplexColumnData(def);
               if(complexData != null) {
                  data.put(def.name.toString(), ((CollectionType)def.type).serializeForNativeProtocol(complexData.iterator(), ProtocolVersion.V3));
               }
            }
         }

         return new UntypedResultSet.Row(data);
      }

      public boolean has(String column) {
         return this.data.get(column) != null;
      }

      public ByteBuffer getBlob(String column) {
         return (ByteBuffer)this.data.get(column);
      }

      public String getString(String column) {
         return (String)UTF8Type.instance.compose((ByteBuffer)this.data.get(column));
      }

      public boolean getBoolean(String column) {
         return ((Boolean)BooleanType.instance.compose((ByteBuffer)this.data.get(column))).booleanValue();
      }

      public byte getByte(String column) {
         return ((Byte)ByteType.instance.compose((ByteBuffer)this.data.get(column))).byteValue();
      }

      public short getShort(String column) {
         return ((Short)ShortType.instance.compose((ByteBuffer)this.data.get(column))).shortValue();
      }

      public int getInt(String column) {
         return ((Integer)Int32Type.instance.compose((ByteBuffer)this.data.get(column))).intValue();
      }

      public double getDouble(String column) {
         return ((Double)DoubleType.instance.compose((ByteBuffer)this.data.get(column))).doubleValue();
      }

      public ByteBuffer getBytes(String column) {
         return (ByteBuffer)this.data.get(column);
      }

      public InetAddress getInetAddress(String column) {
         return (InetAddress)InetAddressType.instance.compose((ByteBuffer)this.data.get(column));
      }

      public UUID getUUID(String column) {
         return (UUID)UUIDType.instance.compose((ByteBuffer)this.data.get(column));
      }

      public Date getTimestamp(String column) {
         return (Date)TimestampType.instance.compose((ByteBuffer)this.data.get(column));
      }

      public long getLong(String column) {
         return ((Long)LongType.instance.compose((ByteBuffer)this.data.get(column))).longValue();
      }

      public <T> Set<T> getSet(String column, AbstractType<T> type) {
         ByteBuffer raw = (ByteBuffer)this.data.get(column);
         return raw == null?null:(Set)SetType.getInstance(type, true).compose(raw);
      }

      public <T> List<T> getList(String column, AbstractType<T> type) {
         ByteBuffer raw = (ByteBuffer)this.data.get(column);
         return raw == null?null:(List)ListType.getInstance(type, true).compose(raw);
      }

      public <K, V> Map<K, V> getMap(String column, AbstractType<K> keyType, AbstractType<V> valueType) {
         ByteBuffer raw = (ByteBuffer)this.data.get(column);
         return raw == null?null:(Map)MapType.getInstance(keyType, valueType, true).compose(raw);
      }

      public Map<String, String> getTextMap(String column) {
         return this.getMap(column, UTF8Type.instance, UTF8Type.instance);
      }

      public <T> Set<T> getFrozenSet(String column, AbstractType<T> type) {
         ByteBuffer raw = (ByteBuffer)this.data.get(column);
         return raw == null?null:(Set)SetType.getInstance(type, false).compose(raw);
      }

      public <T> List<T> getFrozenList(String column, AbstractType<T> type) {
         ByteBuffer raw = (ByteBuffer)this.data.get(column);
         return raw == null?null:(List)ListType.getInstance(type, false).compose(raw);
      }

      public <K, V> Map<K, V> getFrozenMap(String column, AbstractType<K> keyType, AbstractType<V> valueType) {
         ByteBuffer raw = (ByteBuffer)this.data.get(column);
         return raw == null?null:(Map)MapType.getInstance(keyType, valueType, false).compose(raw);
      }

      public Map<String, String> getFrozenTextMap(String column) {
         return this.getFrozenMap(column, UTF8Type.instance, UTF8Type.instance);
      }

      public List<ColumnSpecification> getColumns() {
         return this.columns;
      }

      public String toString() {
         return this.data.toString();
      }
   }

   private static class FromPager extends UntypedResultSet {
      private final SelectStatement select;
      private final QueryPager pager;
      private final PageSize pageSize;
      private final List<ColumnSpecification> metadata;
      private final ReadContext params;

      private FromPager(SelectStatement select, QueryPager pager, PageSize pageSize, ReadContext params) {
         this.select = select;
         this.pager = pager;
         this.pageSize = pageSize;
         this.metadata = select.getResultMetadata().requestNames();
         this.params = params;
      }

      public int size() {
         return this.pageSize.isInRows()?this.pageSize.rawSize():this.pageSize.inEstimatedRows(ResultSet.estimatedRowSizeForColumns(this.select.table, this.select.getSelection().getColumnMapping()));
      }

      public UntypedResultSet.Row one() {
         return (UntypedResultSet.Row)this.iterator().next();
      }

      public Flow<UntypedResultSet.Row> rows() {
         class PagerFlow extends FlowSource<UntypedResultSet.Row> implements FlowSubscription {
            private Iterator<List<ByteBuffer>> currentPage;
            private final int nowInSec = FBUtilities.nowInSeconds();

            PagerFlow() {
            }

            public void requestNext() {
               if(this.currentPage != null && this.currentPage.hasNext()) {
                  this.subscriber.onNext(new UntypedResultSet.Row(FromPager.this.metadata, (List)this.currentPage.next()));
               } else {
                  this.nextPage();
               }

            }

            private void nextPage() {
               if(FromPager.this.pager.isExhausted()) {
                  this.subscriber.onComplete();
               } else {
                  FromPager.this.select.process(FromPager.this.pager.fetchPageInternal(FromPager.this.pageSize), this.nowInSec).map((resultSet) -> {
                     return resultSet.rows;
                  }).subscribe(this::onRows, this::onError);
               }

            }

            private void onRows(List<List<ByteBuffer>> rows) {
               this.currentPage = rows.iterator();
               if(!this.currentPage.hasNext()) {
                  this.requestNext();
               } else {
                  this.subscriber.onNext(new UntypedResultSet.Row(FromPager.this.metadata, (List)this.currentPage.next()));
               }

            }

            private void onError(Throwable error) {
               this.subscriber.onError(error);
            }

            public void close() throws Exception {
            }
         }

         return new PagerFlow();
      }

      public Iterator<UntypedResultSet.Row> iterator() {
         if(TPC.isTPCThread()) {
            throw new TPCUtils.WouldBlockException("Iterating would block TPC thread " + Thread.currentThread().getName());
         } else {
            return new AbstractIterator<UntypedResultSet.Row>() {
               private Iterator<List<ByteBuffer>> currentPage;

               protected UntypedResultSet.Row computeNext() {
                  Flow iter;
                  for(int nowInSec = FBUtilities.nowInSeconds(); this.currentPage == null || !this.currentPage.hasNext(); this.currentPage = ((ResultSet)FromPager.this.select.process(iter, nowInSec).blockingGet()).rows.iterator()) {
                     if(FromPager.this.pager.isExhausted()) {
                        return (UntypedResultSet.Row)this.endOfData();
                     }

                     iter = FromPager.this.params == null?FromPager.this.pager.fetchPageInternal(FromPager.this.pageSize):FromPager.this.pager.fetchPage(FromPager.this.pageSize, FromPager.this.params);
                  }

                  return new UntypedResultSet.Row(FromPager.this.metadata, (List)this.currentPage.next());
               }
            };
         }
      }

      public List<ColumnSpecification> metadata() {
         return this.metadata;
      }
   }

   private static class FromResultList extends UntypedResultSet {
      private final List<Map<String, ByteBuffer>> cqlRows;

      private FromResultList(List<Map<String, ByteBuffer>> cqlRows) {
         this.cqlRows = cqlRows;
      }

      public int size() {
         return this.cqlRows.size();
      }

      public UntypedResultSet.Row one() {
         if(this.cqlRows.size() != 1) {
            throw new IllegalStateException("One row required, " + this.cqlRows.size() + " found");
         } else {
            return new UntypedResultSet.Row((Map)this.cqlRows.get(0));
         }
      }

      public Flow<UntypedResultSet.Row> rows() {
         return Flow.fromIterable(this.cqlRows).map((r) -> {
            return new UntypedResultSet.Row(r);
         });
      }

      public Iterator<UntypedResultSet.Row> iterator() {
         return new AbstractIterator<UntypedResultSet.Row>() {
            Iterator<Map<String, ByteBuffer>> iter;

            {
               this.iter = FromResultList.this.cqlRows.iterator();
            }

            protected UntypedResultSet.Row computeNext() {
               return !this.iter.hasNext()?(UntypedResultSet.Row)this.endOfData():new UntypedResultSet.Row((Map)this.iter.next());
            }
         };
      }

      public List<ColumnSpecification> metadata() {
         throw new UnsupportedOperationException();
      }
   }

   private static class FromResultSet extends UntypedResultSet {
      private final ResultSet cqlRows;

      private FromResultSet(ResultSet cqlRows) {
         this.cqlRows = cqlRows;
      }

      public int size() {
         return this.cqlRows.size();
      }

      public UntypedResultSet.Row one() {
         if(this.cqlRows.size() != 1) {
            throw new IllegalStateException("One row required, " + this.cqlRows.size() + " found");
         } else {
            return new UntypedResultSet.Row(this.cqlRows.metadata.requestNames(), (List)this.cqlRows.rows.get(0));
         }
      }

      public Flow<UntypedResultSet.Row> rows() {
         List<ColumnSpecification> columns = this.cqlRows.metadata.requestNames();
         return Flow.fromIterable(this.cqlRows.rows).map((r) -> {
            return new UntypedResultSet.Row(columns, r);
         });
      }

      public Iterator<UntypedResultSet.Row> iterator() {
         return new AbstractIterator<UntypedResultSet.Row>() {
            Iterator<List<ByteBuffer>> iter;

            {
               this.iter = FromResultSet.this.cqlRows.rows.iterator();
            }

            protected UntypedResultSet.Row computeNext() {
               return !this.iter.hasNext()?(UntypedResultSet.Row)this.endOfData():new UntypedResultSet.Row(FromResultSet.this.cqlRows.metadata.requestNames(), (List)this.iter.next());
            }
         };
      }

      public List<ColumnSpecification> metadata() {
         return this.cqlRows.metadata.requestNames();
      }
   }
}
