package org.apache.cassandra.db.compaction;

import com.google.common.collect.MapMaker;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.ref.WeakReference;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.NoSpamLogger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionLogger {
   private static final JsonNodeFactory json;
   private static final Logger logger;
   private static final CompactionLogger.Writer serializer;
   private final WeakReference<ColumnFamilyStore> cfsRef;
   private final WeakReference<CompactionStrategyManager> csmRef;
   private final AtomicInteger identifier = new AtomicInteger(0);
   private final Map<AbstractCompactionStrategy, String> compactionStrategyMapping = (new MapMaker()).weakKeys().makeMap();
   private final AtomicBoolean enabled = new AtomicBoolean(false);

   public CompactionLogger(ColumnFamilyStore cfs, CompactionStrategyManager csm) {
      this.csmRef = new WeakReference(csm);
      this.cfsRef = new WeakReference(cfs);
   }

   private void forEach(Consumer<AbstractCompactionStrategy> consumer) {
      CompactionStrategyManager csm = (CompactionStrategyManager)this.csmRef.get();
      if(csm != null) {
         csm.getStrategies().forEach((l) -> {
            l.forEach(consumer);
         });
      }
   }

   private ArrayNode compactionStrategyMap(Function<AbstractCompactionStrategy, JsonNode> select) {
      ArrayNode node = json.arrayNode();
      this.forEach((acs) -> {
         node.add((JsonNode)select.apply(acs));
      });
      return node;
   }

   private ArrayNode sstableMap(Collection<SSTableReader> sstables, CompactionLogger.CompactionStrategyAndTableFunction csatf) {
      CompactionStrategyManager csm = (CompactionStrategyManager)this.csmRef.get();
      ArrayNode node = json.arrayNode();
      if(csm == null) {
         return node;
      } else {
         sstables.forEach((t) -> {
            node.add(csatf.apply(csm.getCompactionStrategyFor(t), t));
         });
         return node;
      }
   }

   private String getId(AbstractCompactionStrategy strategy) {
      return (String)this.compactionStrategyMapping.computeIfAbsent(strategy, (s) -> {
         return String.valueOf(this.identifier.getAndIncrement());
      });
   }

   private JsonNode formatSSTables(AbstractCompactionStrategy strategy) {
      ArrayNode node = json.arrayNode();
      CompactionStrategyManager csm = (CompactionStrategyManager)this.csmRef.get();
      ColumnFamilyStore cfs = (ColumnFamilyStore)this.cfsRef.get();
      if(csm != null && cfs != null) {
         Iterator var5 = cfs.getLiveSSTables().iterator();

         while(var5.hasNext()) {
            SSTableReader sstable = (SSTableReader)var5.next();
            if(csm.getCompactionStrategyFor(sstable) == strategy) {
               node.add(this.formatSSTable(strategy, sstable));
            }
         }

         return node;
      } else {
         return node;
      }
   }

   private JsonNode formatSSTable(AbstractCompactionStrategy strategy, SSTableReader sstable) {
      ObjectNode node = json.objectNode();
      node.put("generation", sstable.descriptor.generation);
      node.put("version", sstable.descriptor.version.getVersion());
      node.put("size", sstable.onDiskLength());
      JsonNode logResult = strategy.strategyLogger().sstable(sstable);
      if(logResult != null) {
         node.put("details", logResult);
      }

      return node;
   }

   private JsonNode startStrategy(AbstractCompactionStrategy strategy) {
      ObjectNode node = json.objectNode();
      CompactionStrategyManager csm = (CompactionStrategyManager)this.csmRef.get();
      if(csm == null) {
         return node;
      } else {
         node.put("strategyId", this.getId(strategy));
         node.put("type", strategy.getName());
         node.put("tables", this.formatSSTables(strategy));
         node.put("repaired", csm.isRepaired(strategy));
         List<String> folders = csm.getStrategyFolders(strategy);
         ArrayNode folderNode = json.arrayNode();
         Iterator var6 = folders.iterator();

         while(var6.hasNext()) {
            String folder = (String)var6.next();
            folderNode.add(folder);
         }

         node.put("folders", folderNode);
         JsonNode logResult = strategy.strategyLogger().options();
         if(logResult != null) {
            node.put("options", logResult);
         }

         return node;
      }
   }

   private JsonNode shutdownStrategy(AbstractCompactionStrategy strategy) {
      ObjectNode node = json.objectNode();
      node.put("strategyId", this.getId(strategy));
      return node;
   }

   private JsonNode describeSSTable(AbstractCompactionStrategy strategy, SSTableReader sstable) {
      ObjectNode node = json.objectNode();
      node.put("strategyId", this.getId(strategy));
      node.put("table", this.formatSSTable(strategy, sstable));
      return node;
   }

   private void describeStrategy(ObjectNode node) {
      ColumnFamilyStore cfs = (ColumnFamilyStore)this.cfsRef.get();
      if(cfs != null) {
         node.put("keyspace", cfs.keyspace.getName());
         node.put("table", cfs.getTableName());
         node.put("time", System.currentTimeMillis());
      }
   }

   private JsonNode startStrategies() {
      ObjectNode node = json.objectNode();
      node.put("type", "enable");
      this.describeStrategy(node);
      node.put("strategies", this.compactionStrategyMap(this::startStrategy));
      return node;
   }

   public void enable() {
      if(this.enabled.compareAndSet(false, true)) {
         serializer.writeStart(this.startStrategies(), this);
      }

   }

   public void disable() {
      if(this.enabled.compareAndSet(true, false)) {
         ObjectNode node = json.objectNode();
         node.put("type", "disable");
         this.describeStrategy(node);
         node.put("strategies", this.compactionStrategyMap(this::shutdownStrategy));
         serializer.write(node, this::startStrategies, this);
      }

   }

   public void flush(Collection<SSTableReader> sstables) {
      if(this.enabled.get()) {
         ObjectNode node = json.objectNode();
         node.put("type", "flush");
         this.describeStrategy(node);
         node.put("tables", this.sstableMap(sstables, this::describeSSTable));
         serializer.write(node, this::startStrategies, this);
      }

   }

   public void compaction(long startTime, Collection<SSTableReader> input, long endTime, Collection<SSTableReader> output) {
      if(this.enabled.get()) {
         ObjectNode node = json.objectNode();
         node.put("type", "compaction");
         this.describeStrategy(node);
         node.put("start", String.valueOf(startTime));
         node.put("end", String.valueOf(endTime));
         node.put("input", this.sstableMap(input, this::describeSSTable));
         node.put("output", this.sstableMap(output, this::describeSSTable));
         serializer.write(node, this::startStrategies, this);
      }

   }

   public void pending(AbstractCompactionStrategy strategy, int remaining) {
      if(remaining != 0 && this.enabled.get()) {
         ObjectNode node = json.objectNode();
         node.put("type", "pending");
         this.describeStrategy(node);
         node.put("strategyId", this.getId(strategy));
         node.put("pending", remaining);
         serializer.write(node, this::startStrategies, this);
      }

   }

   static {
      json = JsonNodeFactory.instance;
      logger = LoggerFactory.getLogger(CompactionLogger.class);
      serializer = new CompactionLogger.CompactionLogSerializer();
   }

   private static class CompactionLogSerializer implements CompactionLogger.Writer {
      private static final String logDirectory = System.getProperty("cassandra.logdir", ".");
      private final ExecutorService loggerService;
      private final Set<Object> rolled;
      private OutputStreamWriter stream;

      private CompactionLogSerializer() {
         this.loggerService = Executors.newFixedThreadPool(1);
         this.rolled = new HashSet();
      }

      private static OutputStreamWriter createStream() throws IOException {
         int count = 0;
         Path compactionLog = Paths.get(logDirectory, new String[]{"compaction.log"});
         if(Files.exists(compactionLog, new LinkOption[0])) {
            Path tryPath;
            for(tryPath = compactionLog; Files.exists(tryPath, new LinkOption[0]); tryPath = Paths.get(logDirectory, new String[]{String.format("compaction-%d.log", new Object[]{Integer.valueOf(count++)})})) {
               ;
            }

            Files.move(compactionLog, tryPath, new CopyOption[0]);
         }

         return new OutputStreamWriter(Files.newOutputStream(compactionLog, new OpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE}));
      }

      private void writeLocal(String toWrite) {
         try {
            if(this.stream == null) {
               this.stream = createStream();
            }

            this.stream.write(toWrite);
            this.stream.flush();
         } catch (IOException var3) {
            NoSpamLogger.log(CompactionLogger.logger, NoSpamLogger.Level.ERROR, 1L, TimeUnit.MINUTES, "Could not write to the log file: {}", new Object[]{var3});
         }

      }

      public void writeStart(JsonNode statement, Object tag) {
         String toWrite = statement.toString() + System.lineSeparator();
         this.loggerService.execute(() -> {
            this.rolled.add(tag);
            this.writeLocal(toWrite);
         });
      }

      public void write(JsonNode statement, CompactionLogger.StrategySummary summary, Object tag) {
         String toWrite = statement.toString() + System.lineSeparator();
         this.loggerService.execute(() -> {
            if(!this.rolled.contains(tag)) {
               this.writeLocal(summary.getSummary().toString() + System.lineSeparator());
               this.rolled.add(tag);
            }

            this.writeLocal(toWrite);
         });
      }
   }

   private interface CompactionStrategyAndTableFunction {
      JsonNode apply(AbstractCompactionStrategy var1, SSTableReader var2);
   }

   public interface Writer {
      void writeStart(JsonNode var1, Object var2);

      void write(JsonNode var1, CompactionLogger.StrategySummary var2, Object var3);
   }

   public interface StrategySummary {
      JsonNode getSummary();
   }

   public interface Strategy {
      CompactionLogger.Strategy none = new CompactionLogger.Strategy() {
         public JsonNode sstable(SSTableReader sstable) {
            return null;
         }

         public JsonNode options() {
            return null;
         }
      };

      JsonNode sstable(SSTableReader var1);

      JsonNode options();
   }
}
