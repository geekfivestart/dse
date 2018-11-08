package org.apache.cassandra.hints;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.SyncUtil;

final class HintsCatalog {
   private final File hintsDirectory;
   private final Map<UUID, HintsStore> stores;
   private final ImmutableMap<String, Object> writerParams;

   private HintsCatalog(File hintsDirectory, ImmutableMap<String, Object> writerParams, Map<UUID, List<HintsDescriptor>> descriptors) {
      this.hintsDirectory = hintsDirectory;
      this.writerParams = writerParams;
      this.stores = new ConcurrentHashMap();
      Iterator var4 = descriptors.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<UUID, List<HintsDescriptor>> entry = (Entry)var4.next();
         this.stores.put(entry.getKey(), HintsStore.create((UUID)entry.getKey(), hintsDirectory, writerParams, (List)entry.getValue()));
      }

   }

   static HintsCatalog load(File hintsDirectory, ImmutableMap<String, Object> writerParams) {
      try {
         Map<UUID, List<HintsDescriptor>> stores = (Map)Files.list(hintsDirectory.toPath()).filter(HintsDescriptor::isHintFileName).map(HintsDescriptor::readFromFileQuietly).filter(Optional::isPresent).map(Optional::get).collect(Collectors.groupingBy((h) -> {
            return h.hostId;
         }));
         return new HintsCatalog(hintsDirectory, writerParams, stores);
      } catch (IOException var3) {
         throw new FSReadError(var3, hintsDirectory);
      }
   }

   Stream<HintsStore> stores() {
      return this.stores.values().stream();
   }

   void maybeLoadStores(Iterable<UUID> hostIds) {
      Iterator var2 = hostIds.iterator();

      while(var2.hasNext()) {
         UUID hostId = (UUID)var2.next();
         this.get(hostId);
      }

   }

   HintsStore get(UUID hostId) {
      HintsStore store = (HintsStore)this.stores.get(hostId);
      return store == null?(HintsStore)this.stores.computeIfAbsent(hostId, (id) -> {
         return HintsStore.create(id, this.hintsDirectory, this.writerParams, Collections.emptyList());
      }):store;
   }

   @Nullable
   HintsStore getNullable(UUID hostId) {
      return (HintsStore)this.stores.get(hostId);
   }

   void deleteAllHints() {
      this.stores.keySet().forEach(this::deleteAllHints);
   }

   void deleteAllHints(UUID hostId) {
      HintsStore store = (HintsStore)this.stores.get(hostId);
      if(store != null) {
         store.deleteAllHints();
      }

   }

   boolean hasFiles() {
      return this.stores().anyMatch(HintsStore::hasFiles);
   }

   void exciseStore(UUID hostId) {
      this.deleteAllHints(hostId);
      this.stores.remove(hostId);
   }

   void fsyncDirectory() {
      int fd = NativeLibrary.tryOpenDirectory(this.hintsDirectory.getAbsolutePath());
      if(fd != -1) {
         SyncUtil.trySync(fd);
         NativeLibrary.tryCloseFD(fd);
      }

   }

   ImmutableMap<String, Object> getWriterParams() {
      return this.writerParams;
   }
}
