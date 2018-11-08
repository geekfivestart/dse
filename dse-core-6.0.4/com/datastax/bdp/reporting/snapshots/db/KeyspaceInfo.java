package com.datastax.bdp.reporting.snapshots.db;

import com.datastax.bdp.reporting.CqlWritable;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.ByteBufferUtil;

public class KeyspaceInfo implements Iterable<TableInfo>, CqlWritable {
   public final String name;
   private final Map<String, TableInfo> tables = new HashMap();

   public KeyspaceInfo(String name) {
      this.name = name;
   }

   public TableInfo getTableInfo(String name) {
      return (TableInfo)this.tables.get(name);
   }

   public void addTableInfo(TableInfo info) {
      this.tables.put(info.name, info);
   }

   public Set<String> getTableNames() {
      return this.tables.keySet();
   }

   public void aggregate(KeyspaceInfo ksInfo) {
      Set<String> intersection = Sets.intersection(this.tables.keySet(), ksInfo.getTableNames());
      Iterator var3 = intersection.iterator();

      while(var3.hasNext()) {
         String tableName = (String)var3.next();
         this.addTableInfo(TableInfo.aggregate((TableInfo)this.tables.get(tableName), ksInfo.getTableInfo(tableName)));
      }

      Set<String> onlyInOther = Sets.difference(ksInfo.getTableNames(), this.tables.keySet());
      Iterator var7 = onlyInOther.iterator();

      while(var7.hasNext()) {
         String tableName = (String)var7.next();
         this.addTableInfo(ksInfo.getTableInfo(tableName));
      }

   }

   public Iterator<TableInfo> iterator() {
      return this.tables.values().iterator();
   }

   public List<ByteBuffer> toByteBufferList() {
      long totalReads = 0L;
      long totalWrites = 0L;
      double meanReadLatency = 0.0D;
      double meanWriteLatency = 0.0D;
      long totalDataSize = 0L;
      int tableCount = 0;
      int indexCount = 0;

      Iterator var13;
      TableInfo table;
      for(var13 = this.tables.values().iterator(); var13.hasNext(); totalDataSize += table.totalDataSize) {
         table = (TableInfo)var13.next();
         meanReadLatency = TableInfo.addMeans(meanReadLatency, totalReads, table.meanReadLatency, table.totalReads);
         meanWriteLatency = TableInfo.addMeans(meanWriteLatency, totalWrites, table.meanWriteLatency, table.totalWrites);
         totalReads += table.totalReads;
         totalWrites += table.totalWrites;
      }

      var13 = Keyspace.open(this.name).getColumnFamilyStores().iterator();

      while(var13.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var13.next();
         if(cfs.isIndex()) {
            ++indexCount;
         } else {
            ++tableCount;
         }
      }

      List<ByteBuffer> vars = new ArrayList();
      vars.add(ByteBufferUtil.bytes(this.name));
      vars.add(ByteBufferUtil.bytes(totalReads));
      vars.add(ByteBufferUtil.bytes(totalWrites));
      vars.add(ByteBufferUtil.bytes(meanReadLatency));
      vars.add(ByteBufferUtil.bytes(meanWriteLatency));
      vars.add(ByteBufferUtil.bytes(totalDataSize));
      vars.add(ByteBufferUtil.bytes(tableCount));
      vars.add(ByteBufferUtil.bytes(indexCount));
      return vars;
   }
}
