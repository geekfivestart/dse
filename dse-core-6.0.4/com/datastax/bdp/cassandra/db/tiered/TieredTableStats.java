package com.datastax.bdp.cassandra.db.tiered;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.TieredCompactionStrategy;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public class TieredTableStats implements TieredTableStatsMXBean {
   public static final TieredTableStats instance = new TieredTableStats();

   private TieredTableStats() {
   }

   private boolean usesTieredStorage(ColumnFamilyStore cfs) {
      List<List<AbstractCompactionStrategy>> strategies = cfs.getCompactionStrategyManager().getStrategies();

      assert strategies.size() == 3 : "We're expecting the compaction strategies of data that is repaired / unrepaired / pendingRepairs";

      Iterator var3 = strategies.iterator();

      while(var3.hasNext()) {
         List<AbstractCompactionStrategy> strategyList = (List)var3.next();
         Iterator var5 = strategyList.iterator();

         while(var5.hasNext()) {
            AbstractCompactionStrategy strategy = (AbstractCompactionStrategy)var5.next();
            if(!(strategy instanceof TieredCompactionStrategy)) {
               return false;
            }
         }
      }

      return true;
   }

   private Map<String, String> getSStableInfo(SSTableReader sstable) {
      Map<String, String> sstableInfo = new HashMap();
      sstableInfo.put("size", Long.toString(sstable.bytesOnDisk()));
      sstableInfo.put("estimated_keys", Long.toString(sstable.estimatedKeys()));
      sstableInfo.put("rows", Long.toString(sstable.getTotalRows()));
      sstableInfo.put("max_data_age", Long.toString(sstable.maxDataAge));
      sstableInfo.put("max_timestamp", Long.toString(sstable.getMaxTimestamp()));
      sstableInfo.put("min_timestamp", Long.toString(sstable.getMinTimestamp()));
      sstableInfo.put("level", Integer.toString(sstable.getSSTableLevel()));
      sstableInfo.put("reads_15_min", Double.toString(sstable.getReadMeter().fifteenMinuteRate()));
      sstableInfo.put("reads_120_min", Double.toString(sstable.getReadMeter().twoHourRate()));
      return sstableInfo;
   }

   private Map<String, Map<String, Map<String, String>>> getTieredStorageInfo(ColumnFamilyStore cfs, boolean verbose) {
      Map<String, Map<String, Map<String, String>>> info = new HashMap();
      TieredCompactionStrategy compactionStrategy = (TieredCompactionStrategy)((List)cfs.getCompactionStrategyManager().getStrategies().get(0)).get(0);
      TieredStorageStrategy storageStrategy = compactionStrategy.getStorageStrategy();
      Map<String, TieredTableStats.TierSummary> summaries = new HashMap();
      Iterator var7 = cfs.getLiveSSTables().iterator();

      while(var7.hasNext()) {
         SSTableReader sstable = (SSTableReader)var7.next();
         String tierKey = "orphan";
         Iterator var10 = storageStrategy.getTiers().iterator();

         while(var10.hasNext()) {
            TieredStorageStrategy.Tier tier = (TieredStorageStrategy.Tier)var10.next();
            if(tier.managesPath(sstable.getFilename())) {
               tierKey = Integer.toString(tier.getLevel());
               break;
            }
         }

         if(!summaries.containsKey(tierKey)) {
            summaries.put(tierKey, new TieredTableStats.TierSummary());
         }

         ((TieredTableStats.TierSummary)summaries.get(tierKey)).addSStable(sstable);
         if(!info.containsKey(tierKey)) {
            info.put(tierKey, new HashMap());
         }

         if(verbose) {
            ((Map)info.get(tierKey)).put(sstable.getFilename(), this.getSStableInfo(sstable));
         }
      }

      var7 = summaries.entrySet().iterator();

      while(var7.hasNext()) {
         Entry<String, TieredTableStats.TierSummary> entry = (Entry)var7.next();
         ((Map)info.get(entry.getKey())).put("_summary", ((TieredTableStats.TierSummary)entry.getValue()).getInfo());
      }

      return info;
   }

   public Map<String, Map<String, Map<String, String>>> tierInfo(String keyspace, String table, boolean verbose) {
      TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspace, table);
      if(tableMetadata == null) {
         throw new IllegalArgumentException(String.format("Unknown table: '%s.%s'", new Object[]{keyspace, table}));
      } else {
         ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableMetadata.id);
         if(!this.usesTieredStorage(cfs)) {
            throw new IllegalArgumentException(String.format("The table '%s.%s' is not using tiered storage", new Object[]{keyspace, table}));
         } else {
            return this.getTieredStorageInfo(cfs, verbose);
         }
      }
   }

   public Map<String, Map<String, Map<String, Map<String, Map<String, String>>>>> tierInfo(boolean verbose) {
      Map<String, Map<String, Map<String, Map<String, Map<String, String>>>>> info = new HashMap();
      Iterator var3 = Schema.instance.getKeyspaces().iterator();

      while(var3.hasNext()) {
         String keyspace = (String)var3.next();
         Iterator var5 = Schema.instance.getTablesAndViews(keyspace).iterator();

         while(var5.hasNext()) {
            TableMetadata cfm = (TableMetadata)var5.next();
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
            if(this.usesTieredStorage(cfs)) {
               if(!info.containsKey(keyspace)) {
                  info.put(keyspace, new HashMap());
               }

               ((Map)info.get(keyspace)).put(cfm.name, this.getTieredStorageInfo(cfs, verbose));
            }
         }
      }

      return info;
   }

   private static class TierSummary {
      private long bytes;
      private long maxAge;
      private long maxTimestamp;
      private long minTimestamp;
      private double read15;
      private double read120;
      private int sstables;

      private TierSummary() {
         this.bytes = 0L;
         this.maxAge = 0L;
         this.maxTimestamp = 0L;
         this.minTimestamp = 9223372036854775807L;
         this.read15 = 0.0D;
         this.read120 = 0.0D;
         this.sstables = 0;
      }

      void addSStable(SSTableReader sstable) {
         this.bytes += sstable.bytesOnDisk();
         this.maxAge = Math.max(this.maxAge, sstable.maxDataAge);
         this.maxTimestamp = Math.max(this.maxTimestamp, sstable.getMaxTimestamp());
         this.minTimestamp = Math.min(this.minTimestamp, sstable.getMinTimestamp());
         this.read15 += sstable.getReadMeter().fifteenMinuteRate();
         this.read120 += sstable.getReadMeter().twoHourRate();
         ++this.sstables;
      }

      Map<String, String> getInfo() {
         Map<String, String> info = new HashMap();
         info.put("size", Long.toString(this.bytes));
         info.put("max_data_age", Long.toString(this.maxAge));
         info.put("max_timestamp", Long.toString(this.maxTimestamp));
         info.put("min_timestamp", Long.toString(this.minTimestamp));
         info.put("reads_15_min", Double.toString(this.read15));
         info.put("reads_120_min", Double.toString(this.read120));
         info.put("sstables", Integer.toString(this.sstables));
         return info;
      }
   }
}
