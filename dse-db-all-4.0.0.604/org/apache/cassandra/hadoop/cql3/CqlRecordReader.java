package org.apache.cassandra.hadoop.cql3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlRecordReader extends RecordReader<Long, Row> implements org.apache.hadoop.mapred.RecordReader<Long, Row>, AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(CqlRecordReader.class);
   private ColumnFamilySplit split;
   private CqlRecordReader.RowIterator rowIterator;
   private Pair<Long, Row> currentRow;
   private int totalRowCount;
   private String keyspace;
   private String cfName;
   private String cqlQuery;
   private Cluster cluster;
   private Session session;
   private IPartitioner partitioner;
   private String inputColumns;
   private String userDefinedWhereClauses;
   private List<String> partitionKeys = new ArrayList();
   private LinkedHashMap<String, Boolean> partitionBoundColumns = Maps.newLinkedHashMap();
   protected int nativeProtocolVersion = 1;

   public CqlRecordReader() {
   }

   public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      this.split = (ColumnFamilySplit)split;
      Configuration conf = HadoopCompat.getConfiguration(context);
      this.totalRowCount = this.split.getLength() < 9223372036854775807L?(int)this.split.getLength():ConfigHelper.getInputSplitSize(conf);
      this.cfName = ConfigHelper.getInputColumnFamily(conf);
      this.keyspace = ConfigHelper.getInputKeyspace(conf);
      this.partitioner = ConfigHelper.getInputPartitioner(conf);
      this.inputColumns = CqlConfigHelper.getInputcolumns(conf);
      this.userDefinedWhereClauses = CqlConfigHelper.getInputWhereClauses(conf);

      try {
         if(this.cluster != null) {
            return;
         }

         String[] locations = split.getLocations();
         this.cluster = CqlConfigHelper.getInputCluster(locations, conf);
      } catch (Exception var5) {
         throw new RuntimeException(var5);
      }

      if(this.cluster != null) {
         this.session = this.cluster.connect(this.quote(this.keyspace));
      }

      if(this.session == null) {
         throw new RuntimeException("Can't create connection session");
      } else {
         this.nativeProtocolVersion = this.cluster.getConfiguration().getProtocolOptions().getProtocolVersion().toInt();
         this.cqlQuery = CqlConfigHelper.getInputCql(conf);
         if(!StringUtils.isNotEmpty(this.cqlQuery) || !StringUtils.isNotEmpty(this.inputColumns) && !StringUtils.isNotEmpty(this.userDefinedWhereClauses)) {
            if(StringUtils.isEmpty(this.cqlQuery)) {
               this.cqlQuery = this.buildQuery();
            }

            logger.trace("cqlQuery {}", this.cqlQuery);
            this.rowIterator = new CqlRecordReader.RowIterator();
            logger.trace("created {}", this.rowIterator);
         } else {
            throw new AssertionError("Cannot define a custom query with input columns and / or where clauses");
         }
      }
   }

   public void close() {
      if(this.session != null) {
         this.session.close();
      }

      if(this.cluster != null) {
         this.cluster.close();
      }

   }

   public Long getCurrentKey() {
      return (Long)this.currentRow.left;
   }

   public Row getCurrentValue() {
      return (Row)this.currentRow.right;
   }

   public float getProgress() {
      if(!this.rowIterator.hasNext()) {
         return 1.0F;
      } else {
         float progress = (float)this.rowIterator.totalRead / (float)this.totalRowCount;
         return progress > 1.0F?1.0F:progress;
      }
   }

   public boolean nextKeyValue() throws IOException {
      if(!this.rowIterator.hasNext()) {
         logger.trace("Finished scanning {} rows (estimate was: {})", Integer.valueOf(this.rowIterator.totalRead), Integer.valueOf(this.totalRowCount));
         return false;
      } else {
         try {
            this.currentRow = (Pair)this.rowIterator.next();
            return true;
         } catch (Exception var3) {
            IOException ioe = new IOException(var3.getMessage());
            ioe.initCause(ioe.getCause());
            throw ioe;
         }
      }
   }

   public boolean next(Long key, Row value) throws IOException {
      if(this.nextKeyValue()) {
         ((CqlRecordReader.WrappedRow)value).setRow(this.getCurrentValue());
         return true;
      } else {
         return false;
      }
   }

   public long getPos() throws IOException {
      return (long)this.rowIterator.totalRead;
   }

   public Long createKey() {
      return Long.valueOf(0L);
   }

   public Row createValue() {
      return new CqlRecordReader.WrappedRow();
   }

   public int getNativeProtocolVersion() {
      return this.nativeProtocolVersion;
   }

   private String buildQuery() {
      this.fetchKeys();
      List<String> columns = this.getSelectColumns();
      String selectColumnList = columns.size() == 0?"*":this.makeColumnList(columns);
      String partitionKeyList = this.makeColumnList(this.partitionKeys);
      return String.format("SELECT %s FROM %s.%s WHERE token(%s)>? AND token(%s)<=?" + this.getAdditionalWhereClauses(), new Object[]{selectColumnList, this.quote(this.keyspace), this.quote(this.cfName), partitionKeyList, partitionKeyList});
   }

   private String getAdditionalWhereClauses() {
      String whereClause = "";
      if(StringUtils.isNotEmpty(this.userDefinedWhereClauses)) {
         whereClause = whereClause + " AND " + this.userDefinedWhereClauses;
      }

      if(StringUtils.isNotEmpty(this.userDefinedWhereClauses)) {
         whereClause = whereClause + " ALLOW FILTERING";
      }

      return whereClause;
   }

   private List<String> getSelectColumns() {
      List<String> selectColumns = new ArrayList();
      if(StringUtils.isNotEmpty(this.inputColumns)) {
         selectColumns.addAll(this.partitionKeys);
         Iterator var2 = Splitter.on(',').split(this.inputColumns).iterator();

         while(var2.hasNext()) {
            String column = (String)var2.next();
            if(!this.partitionKeys.contains(column)) {
               selectColumns.add(column);
            }
         }
      }

      return selectColumns;
   }

   private String makeColumnList(Collection<String> columns) {
      return Joiner.on(',').join(Iterables.transform(columns, new Function<String, String>() {
         public String apply(String column) {
            return CqlRecordReader.this.quote(column);
         }
      }));
   }

   private void fetchKeys() {
      TableMetadata tableMetadata = this.session.getCluster().getMetadata().getKeyspace(Metadata.quote(this.keyspace)).getTable(Metadata.quote(this.cfName));
      if(tableMetadata == null) {
         throw new RuntimeException("No table metadata found for " + this.keyspace + "." + this.cfName);
      } else {
         Iterator var2 = tableMetadata.getPartitionKey().iterator();

         while(var2.hasNext()) {
            ColumnMetadata partitionKey = (ColumnMetadata)var2.next();
            this.partitionKeys.add(partitionKey.getName());
         }

      }
   }

   private String quote(String identifier) {
      return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
   }

   private static class WrappedRow implements Row {
      private Row row;

      private WrappedRow() {
      }

      public void setRow(Row row) {
         this.row = row;
      }

      public ColumnDefinitions getColumnDefinitions() {
         return this.row.getColumnDefinitions();
      }

      public boolean isNull(int i) {
         return this.row.isNull(i);
      }

      public boolean isNull(String name) {
         return this.row.isNull(name);
      }

      public Object getObject(int i) {
         return this.row.getObject(i);
      }

      public <T> T get(int i, Class<T> aClass) {
         return this.row.get(i, aClass);
      }

      public <T> T get(int i, TypeToken<T> typeToken) {
         return this.row.get(i, typeToken);
      }

      public <T> T get(int i, TypeCodec<T> typeCodec) {
         return this.row.get(i, typeCodec);
      }

      public Object getObject(String s) {
         return this.row.getObject(s);
      }

      public <T> T get(String s, Class<T> aClass) {
         return this.row.get(s, aClass);
      }

      public <T> T get(String s, TypeToken<T> typeToken) {
         return this.row.get(s, typeToken);
      }

      public <T> T get(String s, TypeCodec<T> typeCodec) {
         return this.row.get(s, typeCodec);
      }

      public boolean getBool(int i) {
         return this.row.getBool(i);
      }

      public boolean getBool(String name) {
         return this.row.getBool(name);
      }

      public short getShort(int i) {
         return this.row.getShort(i);
      }

      public short getShort(String s) {
         return this.row.getShort(s);
      }

      public byte getByte(int i) {
         return this.row.getByte(i);
      }

      public byte getByte(String s) {
         return this.row.getByte(s);
      }

      public int getInt(int i) {
         return this.row.getInt(i);
      }

      public int getInt(String name) {
         return this.row.getInt(name);
      }

      public long getLong(int i) {
         return this.row.getLong(i);
      }

      public long getLong(String name) {
         return this.row.getLong(name);
      }

      public Date getTimestamp(int i) {
         return this.row.getTimestamp(i);
      }

      public Date getTimestamp(String s) {
         return this.row.getTimestamp(s);
      }

      public LocalDate getDate(int i) {
         return this.row.getDate(i);
      }

      public LocalDate getDate(String s) {
         return this.row.getDate(s);
      }

      public long getTime(int i) {
         return this.row.getTime(i);
      }

      public long getTime(String s) {
         return this.row.getTime(s);
      }

      public float getFloat(int i) {
         return this.row.getFloat(i);
      }

      public float getFloat(String name) {
         return this.row.getFloat(name);
      }

      public double getDouble(int i) {
         return this.row.getDouble(i);
      }

      public double getDouble(String name) {
         return this.row.getDouble(name);
      }

      public ByteBuffer getBytesUnsafe(int i) {
         return this.row.getBytesUnsafe(i);
      }

      public ByteBuffer getBytesUnsafe(String name) {
         return this.row.getBytesUnsafe(name);
      }

      public ByteBuffer getBytes(int i) {
         return this.row.getBytes(i);
      }

      public ByteBuffer getBytes(String name) {
         return this.row.getBytes(name);
      }

      public String getString(int i) {
         return this.row.getString(i);
      }

      public String getString(String name) {
         return this.row.getString(name);
      }

      public BigInteger getVarint(int i) {
         return this.row.getVarint(i);
      }

      public BigInteger getVarint(String name) {
         return this.row.getVarint(name);
      }

      public BigDecimal getDecimal(int i) {
         return this.row.getDecimal(i);
      }

      public BigDecimal getDecimal(String name) {
         return this.row.getDecimal(name);
      }

      public UUID getUUID(int i) {
         return this.row.getUUID(i);
      }

      public UUID getUUID(String name) {
         return this.row.getUUID(name);
      }

      public InetAddress getInet(int i) {
         return this.row.getInet(i);
      }

      public InetAddress getInet(String name) {
         return this.row.getInet(name);
      }

      public <T> List<T> getList(int i, Class<T> elementsClass) {
         return this.row.getList(i, elementsClass);
      }

      public <T> List<T> getList(int i, TypeToken<T> typeToken) {
         return this.row.getList(i, typeToken);
      }

      public <T> List<T> getList(String name, Class<T> elementsClass) {
         return this.row.getList(name, elementsClass);
      }

      public <T> List<T> getList(String s, TypeToken<T> typeToken) {
         return this.row.getList(s, typeToken);
      }

      public <T> Set<T> getSet(int i, Class<T> elementsClass) {
         return this.row.getSet(i, elementsClass);
      }

      public <T> Set<T> getSet(int i, TypeToken<T> typeToken) {
         return this.row.getSet(i, typeToken);
      }

      public <T> Set<T> getSet(String name, Class<T> elementsClass) {
         return this.row.getSet(name, elementsClass);
      }

      public <T> Set<T> getSet(String s, TypeToken<T> typeToken) {
         return this.row.getSet(s, typeToken);
      }

      public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
         return this.row.getMap(i, keysClass, valuesClass);
      }

      public <K, V> Map<K, V> getMap(int i, TypeToken<K> typeToken, TypeToken<V> typeToken1) {
         return this.row.getMap(i, typeToken, typeToken1);
      }

      public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
         return this.row.getMap(name, keysClass, valuesClass);
      }

      public <K, V> Map<K, V> getMap(String s, TypeToken<K> typeToken, TypeToken<V> typeToken1) {
         return this.row.getMap(s, typeToken, typeToken1);
      }

      public UDTValue getUDTValue(int i) {
         return this.row.getUDTValue(i);
      }

      public UDTValue getUDTValue(String name) {
         return this.row.getUDTValue(name);
      }

      public TupleValue getTupleValue(int i) {
         return this.row.getTupleValue(i);
      }

      public TupleValue getTupleValue(String name) {
         return this.row.getTupleValue(name);
      }

      public Token getToken(int i) {
         return this.row.getToken(i);
      }

      public Token getToken(String name) {
         return this.row.getToken(name);
      }

      public Token getPartitionKeyToken() {
         return this.row.getPartitionKeyToken();
      }
   }

   private class RowIterator extends AbstractIterator<Pair<Long, Row>> {
      private long keyId = 0L;
      protected int totalRead = 0;
      protected Iterator<Row> rows;
      private Map<String, ByteBuffer> previousRowKey = new HashMap();

      public RowIterator() {
         AbstractType type = CqlRecordReader.this.partitioner.getTokenValidator();
         ResultSet rs = CqlRecordReader.this.session.execute(CqlRecordReader.this.cqlQuery, new Object[]{type.compose(type.fromString(CqlRecordReader.this.split.getStartToken())), type.compose(type.fromString(CqlRecordReader.this.split.getEndToken()))});
         Iterator var4 = CqlRecordReader.this.cluster.getMetadata().getKeyspace(CqlRecordReader.this.quote(CqlRecordReader.this.keyspace)).getTable(CqlRecordReader.this.quote(CqlRecordReader.this.cfName)).getPartitionKey().iterator();

         while(var4.hasNext()) {
            ColumnMetadata meta = (ColumnMetadata)var4.next();
            CqlRecordReader.this.partitionBoundColumns.put(meta.getName(), Boolean.TRUE);
         }

         this.rows = rs.iterator();
      }

      protected Pair<Long, Row> computeNext() {
         if(this.rows != null && this.rows.hasNext()) {
            Row row = (Row)this.rows.next();
            Map<String, ByteBuffer> keyColumns = new HashMap(CqlRecordReader.this.partitionBoundColumns.size());
            Iterator var3 = CqlRecordReader.this.partitionBoundColumns.keySet().iterator();

            String column;
            while(var3.hasNext()) {
               column = (String)var3.next();
               keyColumns.put(column, row.getBytesUnsafe(column));
            }

            if(this.previousRowKey.isEmpty() && !keyColumns.isEmpty()) {
               this.previousRowKey = keyColumns;
               ++this.totalRead;
            } else {
               var3 = CqlRecordReader.this.partitionBoundColumns.keySet().iterator();

               while(var3.hasNext()) {
                  column = (String)var3.next();
                  if(ByteBufferUtil.compareUnsigned((ByteBuffer)keyColumns.get(column), (ByteBuffer)this.previousRowKey.get(column)) != 0) {
                     this.previousRowKey = keyColumns;
                     ++this.totalRead;
                     break;
                  }
               }
            }

            ++this.keyId;
            return Pair.create(Long.valueOf(this.keyId), row);
         } else {
            return (Pair)this.endOfData();
         }
      }
   }
}
