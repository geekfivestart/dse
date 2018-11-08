package org.apache.cassandra.tools;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.streamhist.HistogramDataConsumer;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

public final class Util {
   static final String RESET = "\u001b[0m";
   static final String BLUE = "\u001b[34m";
   static final String CYAN = "\u001b[36m";
   static final String WHITE = "\u001b[37m";
   private static final List<String> ANSI_COLORS = Lists.newArrayList(new String[]{"\u001b[0m", "\u001b[34m", "\u001b[36m", "\u001b[37m"});
   private static final String FULL_BAR_UNICODE = Strings.repeat("▓", 30);
   private static final String EMPTY_BAR_UNICODE = Strings.repeat("░", 30);
   private static final String FULL_BAR_ASCII = Strings.repeat("#", 30);
   private static final String EMPTY_BAR_ASCII = Strings.repeat("-", 30);
   private static final TreeMap<Double, String> BARS_UNICODE = new TreeMap<Double, String>() {
      {
         this.put(Double.valueOf(1.0D), "▉");
         this.put(Double.valueOf(0.875D), "▉");
         this.put(Double.valueOf(0.75D), "▊");
         this.put(Double.valueOf(0.625D), "▋");
         this.put(Double.valueOf(0.375D), "▍");
         this.put(Double.valueOf(0.25D), "▎");
         this.put(Double.valueOf(0.125D), "▏");
      }
   };
   private static final TreeMap<Double, String> BARS_ASCII = new TreeMap<Double, String>() {
      {
         this.put(Double.valueOf(1.0D), "O");
         this.put(Double.valueOf(0.75D), "o");
         this.put(Double.valueOf(0.3D), ".");
      }
   };

   private static TreeMap<Double, String> barmap(boolean unicode) {
      return unicode?BARS_UNICODE:BARS_ASCII;
   }

   public static String progress(double percentComplete, int width, boolean unicode) {
      assert percentComplete >= 0.0D && percentComplete <= 1.0D;

      int cols = (int)(percentComplete * (double)width);
      return (unicode?FULL_BAR_UNICODE:FULL_BAR_ASCII).substring(width - cols) + (unicode?EMPTY_BAR_UNICODE:EMPTY_BAR_ASCII).substring(cols);
   }

   public static String stripANSI(String string) {
      return (String)ANSI_COLORS.stream().reduce(string, (a, b) -> {
         return a.replace(b, "");
      });
   }

   public static int countANSI(String string) {
      return string.length() - stripANSI(string).length();
   }

   public static String wrapQuiet(String toWrap, boolean color) {
      if(Strings.isNullOrEmpty(toWrap)) {
         return "";
      } else {
         StringBuilder sb = new StringBuilder();
         if(color) {
            sb.append("\u001b[37m");
         }

         sb.append("(");
         sb.append(toWrap);
         sb.append(")");
         if(color) {
            sb.append("\u001b[0m");
         }

         return sb.toString();
      }
   }

   private Util() {
   }

   public static void initDatabaseDescriptor() {
      try {
         DatabaseDescriptor.toolInitialization();
      } catch (Throwable var2) {
         boolean logStackTrace = !(var2 instanceof ConfigurationException) || ((ConfigurationException)var2).logStackTrace;
         System.out.println("Exception (" + var2.getClass().getName() + ") encountered during startup: " + var2.getMessage());
         if(logStackTrace) {
            var2.printStackTrace();
            System.exit(3);
         } else {
            System.err.println(var2.getMessage());
            System.exit(3);
         }
      }

   }

   public static <T> Stream<T> iterToStream(Iterator<T> iter) {
      Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, 1024);
      return StreamSupport.stream(splititer, false);
   }

   public static Stream<DecoratedKey> iterToStream(final PartitionIndexIterator iter) {
      Iterator<DecoratedKey> iterator = new Iterator<DecoratedKey>() {
         public boolean hasNext() {
            return iter.key() != null;
         }

         public DecoratedKey next() {
            try {
               DecoratedKey key = iter.key();
               iter.advance();
               return key;
            } catch (Exception var2) {
               throw new RuntimeException(var2);
            }
         }
      };
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 1040), false);
   }

   public static TableMetadata metadataFromSSTable(Descriptor desc) throws IOException {
      return metadataFromSSTable(desc, "keyspace", "table");
   }

   public static TableMetadata metadataFromSSTable(Descriptor desc, String keyspace, String table) throws IOException {
      if(!desc.version.isCompatible()) {
         throw new IOException("Cannot process old and unsupported SSTable version.");
      } else {
         EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
         Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
         SerializationHeader.Component header = (SerializationHeader.Component)sstableMetadata.get(MetadataType.HEADER);
         IPartitioner partitioner = FBUtilities.newPartitioner(desc);
         TableMetadata.Builder builder = TableMetadata.builder(keyspace == null?"keyspace":keyspace, table == null?"table":table).partitioner(partitioner);
         header.getStaticColumns().entrySet().stream().forEach((entry) -> {
            ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString((ByteBuffer)entry.getKey()), true);
            builder.addStaticColumn(ident, (AbstractType)entry.getValue());
         });
         header.getRegularColumns().entrySet().stream().forEach((entry) -> {
            ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString((ByteBuffer)entry.getKey()), true);
            builder.addRegularColumn(ident, (AbstractType)entry.getValue());
         });
         builder.addPartitionKeyColumn("PartitionKey", header.getKeyType());

         for(int i = 0; i < header.getClusteringTypes().size(); ++i) {
            builder.addClusteringColumn("clustering" + (i > 0?Integer.valueOf(i):""), (AbstractType)header.getClusteringTypes().get(i));
         }

         return builder.build();
      }
   }

   public static class TermHistogram {
      public long max;
      public long min;
      public double sum;
      int maxCountLength;
      int maxOffsetLength;
      Map<? extends Number, Long> histogram;
      LongFunction<String> offsetName;
      LongFunction<String> countName;
      String title;

      public TermHistogram(Map<? extends Number, Long> histogram, String title, LongFunction<String> offsetName, LongFunction<String> countName) {
         this.maxCountLength = 5;
         this.maxOffsetLength = 5;
         this.offsetName = offsetName;
         this.countName = countName;
         this.histogram = histogram;
         this.title = title;
         this.maxOffsetLength = title.length();
         histogram.entrySet().stream().forEach((e) -> {
            this.max = Math.max(this.max, ((Long)e.getValue()).longValue());
            this.min = Math.min(this.min, ((Long)e.getValue()).longValue());
            this.sum += (double)((Long)e.getValue()).longValue();
            this.maxCountLength = Math.max(this.maxCountLength, Util.stripANSI((String)countName.apply(((Long)e.getValue()).longValue())).length());
            this.maxOffsetLength = Math.max(this.maxOffsetLength, Util.stripANSI((String)offsetName.apply(((Number)e.getKey()).longValue())).length());
         });
      }

      public TermHistogram(final TombstoneHistogram histogram, String title, LongFunction<String> offsetName, LongFunction<String> countName) {
         this((Map)(new TreeMap<Number, Long>() {
            {
               histogram.forEach((point, value) -> {
                  this.put(Integer.valueOf(point), Long.valueOf((long)value));
               });
            }
         }), title, offsetName, countName);
      }

      public TermHistogram(final EstimatedHistogram histogram, String title, LongFunction<String> offsetName, LongFunction<String> countName) {
         this((Map)(new TreeMap<Number, Long>() {
            {
               long[] counts = histogram.getBuckets(false);
               long[] offsets = histogram.getBucketOffsets();

               for(int i = 0; i < counts.length; ++i) {
                  long e = counts[i];
                  if(e > 0L) {
                     this.put(Long.valueOf(offsets[i]), Long.valueOf(e));
                  }
               }

            }
         }), title, offsetName, countName);
      }

      public String bar(long count, int length, String color, boolean unicode) {
         if(color == null) {
            color = "";
         }

         StringBuilder sb = new StringBuilder(color);
         int intWidth = (int)((double)count * 1.0D / (double)this.max * (double)length);
         double remainderWidth = (double)count * 1.0D / (double)this.max * (double)length - (double)intWidth;
         sb.append(Strings.repeat((String)Util.barmap(unicode).get(Double.valueOf(1.0D)), intWidth));
         if(Util.barmap(unicode).floorKey(Double.valueOf(remainderWidth)) != null) {
            sb.append((String)Util.barmap(unicode).get(Util.barmap(unicode).floorKey(Double.valueOf(remainderWidth))));
         }

         if(!Strings.isNullOrEmpty(color)) {
            sb.append("\u001b[0m");
         }

         return sb.toString();
      }

      public void printHistogram(PrintStream out, boolean color, boolean unicode) {
         int offsetTitleLength = color?this.maxOffsetLength + "\u001b[34m".length():this.maxOffsetLength;
         out.printf("   %-" + offsetTitleLength + "s %s %-" + this.maxCountLength + "s  %s  %sHistogram%s %n", new Object[]{color?"\u001b[34m" + this.title:this.title, color?"\u001b[36m|\u001b[34m":"|", "Count", Util.wrapQuiet("%", color), color?"\u001b[34m":"", color?"\u001b[0m":""});
         this.histogram.entrySet().stream().forEach((e) -> {
            String offset = (String)this.offsetName.apply(((Number)e.getKey()).longValue());
            long count = ((Long)e.getValue()).longValue();
            String histo = this.bar(count, 30, color?"\u001b[37m":null, unicode);
            int mol = color?this.maxOffsetLength + Util.countANSI(offset):this.maxOffsetLength;
            int mcl = color?this.maxCountLength + Util.countANSI((String)this.countName.apply(count)):this.maxCountLength;
            out.printf("   %-" + mol + "s %s %" + mcl + "s %s %s%n", new Object[]{offset, color?"\u001b[36m|\u001b[0m":"|", this.countName.apply(count), Util.wrapQuiet(String.format("%3s", new Object[]{Integer.valueOf((int)(100.0D * ((double)count / this.sum)))}), color), histo});
         });
         EstimatedHistogram eh = new EstimatedHistogram(165);
         Iterator var6 = this.histogram.entrySet().iterator();

         while(var6.hasNext()) {
            Entry<? extends Number, Long> e = (Entry)var6.next();
            eh.add(((Number)e.getKey()).longValue(), ((Long)e.getValue()).longValue());
         }

         String[] percentiles = new String[]{"50th", "75th", "95th", "98th", "99th", "Min", "Max"};
         long[] data = new long[]{eh.percentile(0.5D), eh.percentile(0.75D), eh.percentile(0.95D), eh.percentile(0.98D), eh.percentile(0.99D), eh.min(), eh.max()};
         out.println((color?"\u001b[34m":"") + "   Percentiles" + (color?"\u001b[0m":""));

         for(int i = 0; i < percentiles.length; ++i) {
            out.println(String.format("   %s%-10s%s%s", new Object[]{color?"\u001b[34m":"", percentiles[i], color?"\u001b[0m":"", this.offsetName.apply(data[i])}));
         }

      }
   }
}
