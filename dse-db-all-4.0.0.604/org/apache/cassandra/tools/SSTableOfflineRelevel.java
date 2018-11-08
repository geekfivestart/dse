package org.apache.cassandra.tools;

import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;

public class SSTableOfflineRelevel {
   public SSTableOfflineRelevel() {
   }

   public static void main(String[] args) throws IOException {
      PrintStream out = System.out;
      if(args.length < 2) {
         out.println("This command should be run with Cassandra stopped!");
         out.println("Usage: sstableofflinerelevel [--dry-run] <keyspace> <columnfamily>");
         System.exit(1);
      }

      Util.initDatabaseDescriptor();
      boolean dryRun = args[0].equals("--dry-run");
      String keyspace = args[args.length - 2];
      String columnfamily = args[args.length - 1];
      Schema.instance.loadFromDisk(false);
      if(Schema.instance.getTableMetadataRef(keyspace, columnfamily) == null) {
         throw new IllegalArgumentException(String.format("Unknown keyspace/columnFamily %s.%s", new Object[]{keyspace, columnfamily}));
      } else {
         Keyspace ks = Keyspace.openWithoutSSTables(keyspace);
         ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnfamily);
         Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
         SetMultimap<File, SSTableReader> sstableMultimap = HashMultimap.create();
         Iterator var9 = lister.list().entrySet().iterator();

         while(var9.hasNext()) {
            Entry<Descriptor, Set<Component>> sstable = (Entry)var9.next();
            if(sstable.getKey() != null) {
               try {
                  SSTableReader reader = SSTableReader.open((Descriptor)sstable.getKey());
                  sstableMultimap.put(reader.descriptor.directory, reader);
               } catch (Throwable var12) {
                  out.println("Couldn't open sstable: " + ((Descriptor)sstable.getKey()).filenameFor(Component.DATA));
                  throw Throwables.propagate(var12);
               }
            }
         }

         if(sstableMultimap.isEmpty()) {
            out.println("No sstables to relevel for " + keyspace + "." + columnfamily);
            System.exit(1);
         }

         var9 = sstableMultimap.keySet().iterator();

         while(var9.hasNext()) {
            File directory = (File)var9.next();
            if(!sstableMultimap.get(directory).isEmpty()) {
               SSTableOfflineRelevel.Relevel rl = new SSTableOfflineRelevel.Relevel(sstableMultimap.get(directory));
               out.println("For sstables in " + directory + ":");
               rl.relevel(dryRun);
            }
         }

         System.exit(0);
      }
   }

   private static class Relevel {
      private final Set<SSTableReader> sstables;
      private final int approxExpectedLevels;

      public Relevel(Set<SSTableReader> sstables) {
         this.sstables = sstables;
         this.approxExpectedLevels = (int)Math.ceil(Math.log10((double)sstables.size()));
      }

      private void printLeveling(Iterable<SSTableReader> sstables) {
         Multimap<Integer, SSTableReader> leveling = ArrayListMultimap.create();
         int maxLevel = 0;

         SSTableReader sstable;
         for(Iterator var4 = sstables.iterator(); var4.hasNext(); maxLevel = Math.max(sstable.getSSTableLevel(), maxLevel)) {
            sstable = (SSTableReader)var4.next();
            leveling.put(Integer.valueOf(sstable.getSSTableLevel()), sstable);
         }

         System.out.println("Current leveling:");

         for(int i = 0; i <= maxLevel; ++i) {
            System.out.println(String.format("L%d=%d", new Object[]{Integer.valueOf(i), Integer.valueOf(leveling.get(Integer.valueOf(i)).size())}));
         }

      }

      public void relevel(boolean dryRun) throws IOException {
         this.printLeveling(this.sstables);
         List<SSTableReader> sortedSSTables = new ArrayList(this.sstables);
         Collections.sort(sortedSSTables, new Comparator<SSTableReader>() {
            public int compare(SSTableReader o1, SSTableReader o2) {
               return o1.last.compareTo((PartitionPosition)o2.last);
            }
         });

         Object levels;
         ArrayList level;
         SSTableReader sstable;
         label90:
         for(levels = new ArrayList(); !sortedSSTables.isEmpty(); ((List)levels).add(level)) {
            Iterator<SSTableReader> it = sortedSSTables.iterator();
            level = new ArrayList();
            DecoratedKey lastLast = null;

            while(true) {
               do {
                  if(!it.hasNext()) {
                     continue label90;
                  }

                  sstable = (SSTableReader)it.next();
               } while(lastLast != null && lastLast.compareTo((PartitionPosition)sstable.first) >= 0);

               level.add(sstable);
               lastLast = sstable.last;
               it.remove();
            }
         }

         List<SSTableReader> l0 = new ArrayList();
         int i;
         if(this.approxExpectedLevels < ((List)levels).size()) {
            for(i = this.approxExpectedLevels; i < ((List)levels).size(); ++i) {
               l0.addAll((Collection)((List)levels).get(i));
            }

            levels = ((List)levels).subList(0, this.approxExpectedLevels);
         }

         if(dryRun) {
            System.out.println("Potential leveling: ");
         } else {
            System.out.println("New leveling: ");
         }

         System.out.println("L0=" + l0.size());

         for(i = ((List)levels).size() - 1; i >= 0; --i) {
            System.out.println(String.format("L%d=%d", new Object[]{Integer.valueOf(((List)levels).size() - i), Integer.valueOf(((List)((List)levels).get(i)).size())}));
         }

         if(!dryRun) {
            Iterator var11 = l0.iterator();

            while(var11.hasNext()) {
               SSTableReader sstable = (SSTableReader)var11.next();
               if(sstable.getSSTableLevel() != 0) {
                  sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
               }
            }

            for(i = ((List)levels).size() - 1; i >= 0; --i) {
               Iterator var13 = ((List)((List)levels).get(i)).iterator();

               while(var13.hasNext()) {
                  sstable = (SSTableReader)var13.next();
                  int newLevel = ((List)levels).size() - i;
                  if(newLevel != sstable.getSSTableLevel()) {
                     sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, newLevel);
                  }
               }
            }
         }

      }
   }
}
