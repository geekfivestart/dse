package org.apache.cassandra.io.sstable;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SSTable {
   static final Logger logger = LoggerFactory.getLogger(SSTable.class);
   public static final int TOMBSTONE_HISTOGRAM_BIN_SIZE = 100;
   public static final int TOMBSTONE_HISTOGRAM_SPOOL_SIZE = 100000;
   public static final int TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS = Integer.valueOf(System.getProperty("cassandra.streaminghistogram.roundseconds", "60")).intValue();
   public final Descriptor descriptor;
   protected final Set<Component> components;
   public final boolean compression;
   public DecoratedKey first;
   public DecoratedKey last;
   protected final DiskOptimizationStrategy optimizationStrategy;
   protected final TableMetadataRef metadata;

   protected SSTable(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, DiskOptimizationStrategy optimizationStrategy) {
      assert descriptor != null;

      assert components != null;

      this.descriptor = descriptor;
      Set<Component> dataComponents = new HashSet(components);
      this.compression = dataComponents.contains(Component.COMPRESSION_INFO);
      this.components = new CopyOnWriteArraySet(dataComponents);
      this.metadata = metadata;
      this.optimizationStrategy = (DiskOptimizationStrategy)Objects.requireNonNull(optimizationStrategy);
   }

   public static boolean delete(Descriptor desc, Set<Component> components) {
      logger.debug("Deleting sstable: {}", desc);
      if(components.contains(Component.DATA)) {
         FileUtils.deleteWithConfirm(desc.filenameFor(Component.DATA));
      }

      Iterator var2 = components.iterator();

      while(var2.hasNext()) {
         Component component = (Component)var2.next();
         if(!component.equals(Component.DATA)) {
            FileUtils.deleteWithConfirm(desc.filenameFor(component));
         }
      }

      return true;
   }

   public TableMetadata metadata() {
      return this.metadata.get();
   }

   public IPartitioner getPartitioner() {
      return this.metadata().partitioner;
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      return this.getPartitioner().decorateKey(key);
   }

   public static DecoratedKey getMinimalKey(DecoratedKey key) {
      return (DecoratedKey)(key.getKey().position() <= 0 && !key.getKey().hasRemaining() && key.getKey().hasArray()?key:new BufferDecoratedKey(key.getToken(), HeapAllocator.instance.clone(key.getKey())));
   }

   public String getFilename() {
      return this.descriptor.filenameFor(Component.DATA);
   }

   public String getColumnFamilyName() {
      return this.descriptor.cfname;
   }

   public String getKeyspaceName() {
      return this.descriptor.ksname;
   }

   public List<String> getAllFilePaths() {
      List<String> ret = new ArrayList(this.components.size());
      Iterator var2 = this.components.iterator();

      while(var2.hasNext()) {
         Component component = (Component)var2.next();
         ret.add(this.descriptor.filenameFor(component));
      }

      return ret;
   }

   public static Pair<Descriptor, Component> tryComponentFromFilename(File file) {
      try {
         return Descriptor.fromFilenameWithComponent(file);
      } catch (Throwable var2) {
         return null;
      }
   }

   public static Descriptor tryDescriptorFromFilename(File file) {
      try {
         return Descriptor.fromFilename(file);
      } catch (Throwable var2) {
         return null;
      }
   }

   public static Set<Component> componentsFor(Descriptor desc) {
      try {
         try {
            return readTOC(desc);
         } catch (FileNotFoundException var3) {
            Set<Component> components = discoverComponentsFor(desc);
            if(components.isEmpty()) {
               return components;
            } else {
               if(!components.contains(Component.TOC)) {
                  components.add(Component.TOC);
               }

               appendTOC(desc, components);
               return components;
            }
         }
      } catch (IOException var4) {
         throw new IOError(var4);
      }
   }

   public static Set<Component> discoverComponentsFor(Descriptor desc) {
      Set<Component.Type> knownTypes = Sets.difference(Component.TYPES, Collections.singleton(Component.Type.CUSTOM));
      Set<Component> components = Sets.newHashSetWithExpectedSize(knownTypes.size());
      Iterator var3 = knownTypes.iterator();

      while(var3.hasNext()) {
         Component.Type componentType = (Component.Type)var3.next();
         Component component = new Component(componentType);
         if((new File(desc.filenameFor(component))).exists()) {
            components.add(component);
         }
      }

      return components;
   }

   public long bytesOnDisk() {
      long bytes = 0L;

      Component component;
      for(Iterator var3 = this.components.iterator(); var3.hasNext(); bytes += (new File(this.descriptor.filenameFor(component))).length()) {
         component = (Component)var3.next();
      }

      return bytes;
   }

   public String toString() {
      return this.getClass().getSimpleName() + "(path='" + this.getFilename() + '\'' + ')';
   }

   protected static Set<Component> readTOC(Descriptor descriptor) throws IOException {
      File tocFile = new File(descriptor.filenameFor(Component.TOC));
      List<String> componentNames = Files.readLines(tocFile, Charset.defaultCharset());
      Set<Component> components = Sets.newHashSetWithExpectedSize(componentNames.size());
      Iterator var4 = componentNames.iterator();

      while(var4.hasNext()) {
         String componentName = (String)var4.next();
         Component component = new Component(Component.Type.fromRepresentation(componentName), componentName);
         if(!(new File(descriptor.filenameFor(component))).exists()) {
            logger.error("Missing component: {}", descriptor.filenameFor(component));
         } else {
            components.add(component);
         }
      }

      return components;
   }

   protected static void appendTOC(Descriptor descriptor, Collection<Component> components) {
      File tocFile = new File(descriptor.filenameFor(Component.TOC));

      try {
         PrintWriter w = new PrintWriter(new FileWriter(tocFile, true));
         Throwable var4 = null;

         try {
            Iterator var5 = components.iterator();

            while(var5.hasNext()) {
               Component component = (Component)var5.next();
               w.println(component.name);
            }
         } catch (Throwable var15) {
            var4 = var15;
            throw var15;
         } finally {
            if(w != null) {
               if(var4 != null) {
                  try {
                     w.close();
                  } catch (Throwable var14) {
                     var4.addSuppressed(var14);
                  }
               } else {
                  w.close();
               }
            }

         }

      } catch (IOException var17) {
         throw new FSWriteError(var17, tocFile);
      }
   }

   public synchronized void addComponents(Collection<Component> newComponents) {
      Collection<Component> componentsToAdd = Collections2.filter(newComponents, Predicates.not(Predicates.in(this.components)));
      appendTOC(this.descriptor, componentsToAdd);
      this.components.addAll(componentsToAdd);
   }

   public AbstractBounds<Token> getBounds() {
      return AbstractBounds.bounds(this.first.getToken(), true, this.last.getToken(), true);
   }
}
