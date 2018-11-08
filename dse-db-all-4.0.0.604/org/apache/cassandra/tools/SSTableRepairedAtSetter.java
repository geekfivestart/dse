package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;

public class SSTableRepairedAtSetter {
   public SSTableRepairedAtSetter() {
   }

   public static void main(String[] args) throws IOException {
      PrintStream out = System.out;
      if(args.length == 0) {
         out.println("This command should be run with Cassandra stopped!");
         out.println("Usage: sstablerepairedset [--is-repaired | --is-unrepaired] [-f <sstable-list> | <sstables>]");
         System.exit(1);
      }

      if(args.length < 3 || !args[0].equals("--really-set") || !args[1].equals("--is-repaired") && !args[1].equals("--is-unrepaired")) {
         out.println("This command should be run with Cassandra stopped, otherwise you will get very strange behavior");
         out.println("Verify that Cassandra is not running and then execute the command like this:");
         out.println("Usage: sstablerepairedset --really-set [--is-repaired | --is-unrepaired] [-f <sstable-list> | <sstables>]");
         System.exit(1);
      }

      Util.initDatabaseDescriptor();
      boolean setIsRepaired = args[1].equals("--is-repaired");
      List fileNames;
      if(args[2].equals("-f")) {
         fileNames = Files.readAllLines(Paths.get(args[3], new String[0]), Charset.defaultCharset());
      } else {
         fileNames = Arrays.asList(args).subList(2, args.length);
      }

      Iterator var4 = fileNames.iterator();

      while(var4.hasNext()) {
         String fname = (String)var4.next();
         Descriptor descriptor = Descriptor.fromFilename(fname);
         if(!descriptor.version.isCompatible()) {
            System.err.println("SSTable " + fname + " is in a old and unsupported format");
         } else if(setIsRepaired) {
            FileTime f = Files.getLastModifiedTime((new File(descriptor.filenameFor(Component.DATA))).toPath(), new LinkOption[0]);
            descriptor.getMetadataSerializer().mutateRepaired(descriptor, f.toMillis(), (UUID)null);
         } else {
            descriptor.getMetadataSerializer().mutateRepaired(descriptor, 0L, (UUID)null);
         }
      }

   }
}
