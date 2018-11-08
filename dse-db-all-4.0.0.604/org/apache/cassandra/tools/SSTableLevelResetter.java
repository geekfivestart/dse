package org.apache.cassandra.tools;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class SSTableLevelResetter {
   public SSTableLevelResetter() {
   }

   public static void main(String[] args) {
      PrintStream out = System.out;
      if(args.length == 0) {
         out.println("This command should be run with Cassandra stopped!");
         out.println("Usage: sstablelevelreset <keyspace> <table>");
         System.exit(1);
      }

      if(!args[0].equals("--really-reset") || args.length != 3) {
         out.println("This command should be run with Cassandra stopped, otherwise you will get very strange behavior");
         out.println("Verify that Cassandra is not running and then execute the command like this:");
         out.println("Usage: sstablelevelreset --really-reset <keyspace> <table>");
         System.exit(1);
      }

      Util.initDatabaseDescriptor();

      try {
         Schema.instance.loadFromDisk(false);
         String keyspaceName = args[1];
         String columnfamily = args[2];
         if(Schema.instance.getTableMetadataRef(keyspaceName, columnfamily) == null) {
            System.err.println("ColumnFamily not found: " + keyspaceName + "/" + columnfamily);
            System.exit(1);
         }

         Keyspace keyspace = Keyspace.openWithoutSSTables(keyspaceName);
         ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnfamily);
         boolean foundSSTable = false;
         Iterator var7 = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().entrySet().iterator();

         while(var7.hasNext()) {
            Entry<Descriptor, Set<Component>> sstable = (Entry)var7.next();
            if(((Set)sstable.getValue()).contains(Component.STATS)) {
               foundSSTable = true;
               Descriptor descriptor = (Descriptor)sstable.getKey();
               StatsMetadata metadata = (StatsMetadata)descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
               if(metadata.sstableLevel > 0) {
                  out.println("Changing level from " + metadata.sstableLevel + " to 0 on " + descriptor.filenameFor(Component.DATA));
                  descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
               } else {
                  out.println("Skipped " + descriptor.filenameFor(Component.DATA) + " since it is already on level 0");
               }
            }
         }

         if(!foundSSTable) {
            out.println("Found no sstables, did you give the correct keyspace/table?");
         }
      } catch (Throwable var11) {
         JVMStabilityInspector.inspectThrowable(var11);
         var11.printStackTrace();
         System.exit(1);
      }

      System.exit(0);
   }
}
