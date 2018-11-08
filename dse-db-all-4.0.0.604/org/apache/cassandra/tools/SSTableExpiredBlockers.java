package org.apache.cassandra.tools;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public class SSTableExpiredBlockers {
   public SSTableExpiredBlockers() {
   }

   public static void main(String[] args) {
      PrintStream out = System.out;
      if(args.length < 2) {
         out.println("Usage: sstableexpiredblockers <keyspace> <table>");
         System.exit(1);
      }

      Util.initDatabaseDescriptor();
      String keyspace = args[args.length - 2];
      String columnfamily = args[args.length - 1];
      Schema.instance.loadFromDisk(false);
      TableMetadata metadata = Schema.instance.validateTable(keyspace, columnfamily);
      Keyspace ks = Keyspace.openWithoutSSTables(keyspace);
      ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnfamily);
      Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
      Set<SSTableReader> sstables = new HashSet();
      Iterator var9 = lister.list().entrySet().iterator();

      while(var9.hasNext()) {
         Entry<Descriptor, Set<Component>> sstable = (Entry)var9.next();
         if(sstable.getKey() != null) {
            try {
               SSTableReader reader = SSTableReader.open((Descriptor)sstable.getKey());
               sstables.add(reader);
            } catch (Throwable var13) {
               out.println("Couldn't open sstable: " + ((Descriptor)sstable.getKey()).filenameFor(Component.DATA) + " (" + var13.getMessage() + ")");
            }
         }
      }

      if(sstables.isEmpty()) {
         out.println("No sstables for " + keyspace + "." + columnfamily);
         System.exit(1);
      }

      int gcBefore = (int)(System.currentTimeMillis() / 1000L) - metadata.params.gcGraceSeconds;
      Multimap<SSTableReader, SSTableReader> blockers = checkForExpiredSSTableBlockers(sstables, gcBefore);
      Iterator var16 = blockers.keySet().iterator();

      while(var16.hasNext()) {
         SSTableReader blocker = (SSTableReader)var16.next();
         out.println(String.format("%s blocks %d expired sstables from getting dropped: %s%n", new Object[]{formatForExpiryTracing(Collections.singleton(blocker)), Integer.valueOf(blockers.get(blocker).size()), formatForExpiryTracing(blockers.get(blocker))}));
      }

      System.exit(0);
   }

   public static Multimap<SSTableReader, SSTableReader> checkForExpiredSSTableBlockers(Iterable<SSTableReader> sstables, int gcBefore) {
      Multimap<SSTableReader, SSTableReader> blockers = ArrayListMultimap.create();
      Iterator var3 = sstables.iterator();

      while(true) {
         SSTableReader sstable;
         do {
            if(!var3.hasNext()) {
               return blockers;
            }

            sstable = (SSTableReader)var3.next();
         } while(sstable.getSSTableMetadata().maxLocalDeletionTime >= gcBefore);

         Iterator var5 = sstables.iterator();

         while(var5.hasNext()) {
            SSTableReader potentialBlocker = (SSTableReader)var5.next();
            if(!potentialBlocker.equals(sstable) && potentialBlocker.getMinTimestamp() <= sstable.getMaxTimestamp() && potentialBlocker.getSSTableMetadata().maxLocalDeletionTime > gcBefore) {
               blockers.put(potentialBlocker, sstable);
            }
         }
      }
   }

   private static String formatForExpiryTracing(Iterable<SSTableReader> sstables) {
      StringBuilder sb = new StringBuilder();
      Iterator var2 = sstables.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();
         sb.append(String.format("[%s (minTS = %d, maxTS = %d, maxLDT = %d)]", new Object[]{sstable, Long.valueOf(sstable.getMinTimestamp()), Long.valueOf(sstable.getMaxTimestamp()), Integer.valueOf(sstable.getSSTableMetadata().maxLocalDeletionTime)})).append(", ");
      }

      return sb.toString();
   }
}
