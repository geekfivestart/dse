package org.apache.cassandra.tools;

import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.trieindex.PartitionIndex;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class IndexAnalyzer {
   private static final Options options = new Options();
   private static CommandLine cmd;

   public IndexAnalyzer() {
   }

   public static void main(String[] args) throws ConfigurationException {
      PosixParser parser = new PosixParser();

      try {
         cmd = parser.parse(options, args);
      } catch (ParseException var61) {
         System.err.println(var61.getMessage());
         printUsage();
         System.exit(1);
      }

      if(cmd.getArgs().length != 1) {
         System.err.println("You must supply exactly one sstable index");
         printUsage();
         System.exit(1);
      }

      String ssTableFileName = (new File(cmd.getArgs()[0])).getAbsolutePath();
      if(!(new File(ssTableFileName)).exists()) {
         System.err.println("Cannot find file " + ssTableFileName);
         System.exit(1);
      }

      Descriptor desc = Descriptor.fromFilename(ssTableFileName);
      String fname = ssTableFileName.contains("Partitions")?ssTableFileName:desc.filenameFor(Component.PARTITION_INDEX);

      try {
         FileHandle.Builder fhBuilder = (new FileHandle.Builder(fname)).bufferSize(4096);
         Throwable var6 = null;

         try {
            PartitionIndex index = PartitionIndex.load(fhBuilder, (IPartitioner)null, false);
            Throwable var8 = null;

            try {
               IndexAnalyzer.Analyzer analyzer = new IndexAnalyzer.Analyzer(index, Rebufferer.ReaderConstraint.NONE);
               Throwable var10 = null;

               try {
                  analyzer.run();
                  analyzer.printResults();
               } catch (Throwable var60) {
                  var10 = var60;
                  throw var60;
               } finally {
                  if(analyzer != null) {
                     if(var10 != null) {
                        try {
                           analyzer.close();
                        } catch (Throwable var59) {
                           var10.addSuppressed(var59);
                        }
                     } else {
                        analyzer.close();
                     }
                  }

               }
            } catch (Throwable var63) {
               var8 = var63;
               throw var63;
            } finally {
               if(index != null) {
                  if(var8 != null) {
                     try {
                        index.close();
                     } catch (Throwable var58) {
                        var8.addSuppressed(var58);
                     }
                  } else {
                     index.close();
                  }
               }

            }
         } catch (Throwable var65) {
            var6 = var65;
            throw var65;
         } finally {
            if(fhBuilder != null) {
               if(var6 != null) {
                  try {
                     fhBuilder.close();
                  } catch (Throwable var57) {
                     var6.addSuppressed(var57);
                  }
               } else {
                  fhBuilder.close();
               }
            }

         }
      } catch (IOException var67) {
         var67.printStackTrace(System.err);
      }

      System.exit(0);
   }

   private static void printUsage() {
      String usage = String.format("analyzeindex <options> <sstable file path>%n", new Object[0]);
      String header = "Print an analysis of the sstable index.";
      (new HelpFormatter()).printHelp(usage, header, options, "");
   }

   private static void printHistogram(List<AtomicLong> list, String string) {
      System.out.println(string);
      long totalCount = list.stream().mapToLong(AtomicLong::get).sum();
      long sum = 0L;

      for(int i = 0; i < list.size(); ++i) {
         long count = ((AtomicLong)list.get(i)).get();
         if(count != 0L) {
            System.out.format("%s %3d: %,12d (%5.2f%%)\n", new Object[]{string, Integer.valueOf(i + 1), Long.valueOf(count), Double.valueOf((double)count * 100.0D / (double)totalCount)});
            sum += count * (long)(i + 1);
         }
      }

      System.out.format("%s mean %.2f\n", new Object[]{string, Double.valueOf((double)sum * 1.0D / (double)totalCount)});
   }

   static {
      DatabaseDescriptor.toolInitialization();
   }

   static class Analyzer extends PartitionIndex.Reader {
      List<IndexAnalyzer.LevelStats> levelStats = new ArrayList();
      List<IndexAnalyzer.LevelStats> bottomUpStats = new ArrayList();
      List<AtomicLong> countPerDepth = new ArrayList();
      List<AtomicLong> countPerPageDepth = new ArrayList();

      Analyzer(PartitionIndex index, Rebufferer.ReaderConstraint rc) {
         super(index, rc);
      }

      void run() {
         this.run(this.root, -1L, -1, -1);
      }

      int run(long node, long prevNode, int depth, int pageDepth) {
         this.go(node);
         ++depth;
         int pageId = Ints.checkedCast(node >> 12);
         boolean newPage = (long)pageId != prevNode >> 12;
         if(newPage) {
            ++pageDepth;
         }

         IndexAnalyzer.LevelStats ls = this.levelStats(this.levelStats, pageDepth);
         this.updateLevelStats(ls, depth, pageId, newPage);
         if(this.payloadFlags() != 0) {
            this.incCount(this.countPerDepth, depth);
            this.incCount(this.countPerPageDepth, pageDepth);
         }

         int tr = this.transitionRange();
         int bottomUpLevel = 0;

         for(int i = 0; i < tr; ++i) {
            long child = this.transition(i);
            if(child != -1L) {
               int childblevel = this.run(child, node, depth, pageDepth);
               bottomUpLevel = Math.max(bottomUpLevel, childblevel);
               this.go(node);
            }
         }

         IndexAnalyzer.LevelStats bup = this.levelStats(this.bottomUpStats, bottomUpLevel);
         this.updateLevelStats(bup, depth, pageId, newPage);
         if(newPage) {
            ++bottomUpLevel;
         }

         return bottomUpLevel;
      }

      void updateLevelStats(IndexAnalyzer.LevelStats ls, int depth, int pageId, boolean newPage) {
         if(newPage) {
            this.incCount(ls.enterDepthHistogram, depth);
         }

         ls.pages.set(pageId);
         ++ls.countPerType[this.nodeTypeOrdinal()];
         long[] var10000 = ls.bytesPerType;
         int var10001 = this.nodeTypeOrdinal();
         var10000[var10001] += (long)this.nodeSize();
         if(this.payloadFlags() != 0) {
            ++ls.payloadCount;
            ls.bytesInPayload += (long)this.payloadSize();
         }

      }

      private void incCount(List<AtomicLong> list, int index) {
         while(index >= list.size()) {
            list.add(new AtomicLong());
         }

         ((AtomicLong)list.get(index)).incrementAndGet();
      }

      private IndexAnalyzer.LevelStats levelStats(List<IndexAnalyzer.LevelStats> levelStats, int pageDepth) {
         while(pageDepth >= levelStats.size()) {
            levelStats.add(new IndexAnalyzer.LevelStats());
         }

         return (IndexAnalyzer.LevelStats)levelStats.get(pageDepth);
      }

      void printResults() {
         IndexAnalyzer.LevelStats combined = new IndexAnalyzer.LevelStats();
         Iterator var2 = this.levelStats.iterator();

         while(var2.hasNext()) {
            IndexAnalyzer.LevelStats level = (IndexAnalyzer.LevelStats)var2.next();
            combined.join(level);
         }

         int lvl = 0;
         int combinedPages = combined.pages.cardinality();
         Iterator var4 = this.levelStats.iterator();

         IndexAnalyzer.LevelStats level;
         while(var4.hasNext()) {
            level = (IndexAnalyzer.LevelStats)var4.next();
            StringBuilder var10001 = (new StringBuilder()).append("at page level ");
            ++lvl;
            level.print(var10001.append(lvl).toString(), combined, combinedPages);
         }

         lvl = 0;
         var4 = this.bottomUpStats.iterator();

         while(var4.hasNext()) {
            level = (IndexAnalyzer.LevelStats)var4.next();
            level.print("at bottom-up level " + lvl++, combined, combinedPages);
         }

         combined.print("for all levels", combined, combinedPages);
         IndexAnalyzer.printHistogram(this.countPerDepth, "Depth");
         IndexAnalyzer.printHistogram(this.countPerPageDepth, "Page depth");
      }
   }

   static class LevelStats {
      BitSet pages = new BitSet();
      long[] countPerType = new long[16];
      long[] bytesPerType = new long[16];
      long bytesInPayload = 0L;
      long payloadCount = 0L;
      List<AtomicLong> enterDepthHistogram = new ArrayList();

      LevelStats() {
      }

      public void join(IndexAnalyzer.LevelStats level) {
         this.pages.or(level.pages);

         for(int l = 0; l < this.countPerType.length; ++l) {
            this.countPerType[l] += level.countPerType[l];
            this.bytesPerType[l] += level.bytesPerType[l];
         }

         this.bytesInPayload += level.bytesInPayload;
         this.payloadCount += level.payloadCount;
      }

      public static String humanReadable(long bytes, boolean si) {
         int unit = si?1000:1024;
         int exp = (int)(Math.log((double)bytes) / Math.log((double)unit));
         exp = Math.max(0, exp);
         String pre = (si?" kMGTPE":" KMGTPE").charAt(exp) + (si?"":(exp > 0?"i":" "));
         return String.format("%.1f %s", new Object[]{Double.valueOf((double)bytes / Math.pow((double)unit, (double)exp)), pre});
      }

      public void print(String string, IndexAnalyzer.LevelStats combined, int combinedPages) {
         System.out.println("Node statistics " + string);
         int pc = this.pages.cardinality();
         System.out.format("Page count %,d (%sb, %.1f%%)\n", new Object[]{Integer.valueOf(pc), humanReadable((long)pc * 4096L, false), Double.valueOf((double)pc * 100.0D / (double)combinedPages)});
         long allBytes = (long)combinedPages * 4096L;
         long levelBytes = (long)pc * 4096L;
         long nodeBytes = Arrays.stream(this.bytesPerType).sum();
         long nodeCount = Arrays.stream(this.countPerType).sum();

         for(int i = 0; i < this.countPerType.length; ++i) {
            if(this.countPerType[i] != 0L) {
               System.out.format("%15s count %,12d, bytes %s\n", new Object[]{TrieNode.nodeTypeString(i), Long.valueOf(this.countPerType[i]), this.bytePrint(this.bytesPerType[i], levelBytes, allBytes)});
            }
         }

         System.out.format("%15s count %,12d, bytes %s\n", new Object[]{"All nodes", Long.valueOf(nodeCount), this.bytePrint(nodeBytes, levelBytes, allBytes)});
         System.out.format("%15s count %,12d, bytes %s\n", new Object[]{"Payload", Long.valueOf(this.payloadCount), this.bytePrint(this.bytesInPayload, levelBytes, allBytes)});
         System.out.format("%15s count %,12d, bytes %s\n", new Object[]{"Altogether", Long.valueOf(nodeCount), this.bytePrint(this.bytesInPayload + nodeBytes, levelBytes, allBytes)});
         if(!this.enterDepthHistogram.isEmpty()) {
            IndexAnalyzer.printHistogram(this.enterDepthHistogram, "Enter depths");
         }

         System.out.println();
      }

      private String bytePrint(long l, long levelBytes, long allBytes) {
         return String.format("%8sb (%5.2f%% level, %5.2f%% all)", new Object[]{humanReadable(l, false), Double.valueOf((double)l * 100.0D / (double)levelBytes), Double.valueOf((double)l * 100.0D / (double)allBytes)});
      }
   }
}
