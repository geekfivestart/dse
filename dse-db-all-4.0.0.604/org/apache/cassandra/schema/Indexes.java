package org.apache.cassandra.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.cassandra.exceptions.ConfigurationException;

public final class Indexes implements Iterable<IndexMetadata> {
   private final ImmutableMap<String, IndexMetadata> indexesByName;
   private final ImmutableMap<UUID, IndexMetadata> indexesById;

   private Indexes(Indexes.Builder builder) {
      this.indexesByName = builder.indexesByName.build();
      this.indexesById = builder.indexesById.build();
   }

   public static Indexes.Builder builder() {
      return new Indexes.Builder();
   }

   public static Indexes none() {
      return builder().build();
   }

   public static Indexes of(IndexMetadata... indexes) {
      return builder().add(indexes).build();
   }

   public static Indexes of(Iterable<IndexMetadata> indexes) {
      return builder().add(indexes).build();
   }

   public Iterator<IndexMetadata> iterator() {
      return this.indexesByName.values().iterator();
   }

   public Stream<IndexMetadata> stream() {
      return this.indexesById.values().stream();
   }

   public int size() {
      return this.indexesByName.size();
   }

   public boolean isEmpty() {
      return this.indexesByName.isEmpty();
   }

   public Optional<IndexMetadata> get(String name) {
      return Optional.ofNullable(this.indexesByName.get(name));
   }

   public boolean has(String name) {
      return this.indexesByName.containsKey(name);
   }

   public Optional<IndexMetadata> get(UUID id) {
      return Optional.ofNullable(this.indexesById.get(id));
   }

   public boolean has(UUID id) {
      return this.indexesById.containsKey(id);
   }

   public Indexes with(IndexMetadata index) {
      if(this.get(index.name).isPresent()) {
         throw new IllegalStateException(String.format("Index %s already exists", new Object[]{index.name}));
      } else {
         return builder().add((Iterable)this).add(index).build();
      }
   }

   public Indexes without(String name) {
      IndexMetadata index = (IndexMetadata)this.get(name).orElseThrow(() -> {
         return new IllegalStateException(String.format("Index %s doesn't exist", new Object[]{name}));
      });
      return builder().add(Iterables.filter(this, (v) -> {
         return v != index;
      })).build();
   }

   public Indexes replace(IndexMetadata index) {
      return this.without(index.name).with(index);
   }

   public boolean equals(Object o) {
      return this == o || o instanceof Indexes && this.indexesByName.equals(((Indexes)o).indexesByName);
   }

   public void validate(TableMetadata table) {
      Set<String> indexNames = Sets.newHashSetWithExpectedSize(this.indexesByName.size());
      UnmodifiableIterator var3 = this.indexesByName.values().iterator();

      while(var3.hasNext()) {
         IndexMetadata index = (IndexMetadata)var3.next();
         if(indexNames.contains(index.name)) {
            throw new ConfigurationException(String.format("Duplicate index name %s for table %s", new Object[]{index.name, table}));
         }

         indexNames.add(index.name);
      }

      this.indexesByName.values().forEach((i) -> {
         i.validate(table);
      });
   }

   public int hashCode() {
      return this.indexesByName.hashCode();
   }

   public String toString() {
      return this.indexesByName.values().toString();
   }

   public static String getAvailableIndexName(String ksName, String cfName, String indexNameRoot) {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);
      Set<String> existingNames = ksm == null?new HashSet():ksm.existingIndexNames((String)null);
      String baseName = IndexMetadata.getDefaultIndexName(cfName, indexNameRoot);
      String acceptedName = baseName;

      StringBuilder var10000;
      for(int i = 0; ((Set)existingNames).contains(acceptedName); acceptedName = var10000.append(i).toString()) {
         var10000 = (new StringBuilder()).append(baseName).append('_');
         ++i;
      }

      return acceptedName;
   }

   public static final class Builder {
      final com.google.common.collect.ImmutableMap.Builder<String, IndexMetadata> indexesByName;
      final com.google.common.collect.ImmutableMap.Builder<UUID, IndexMetadata> indexesById;

      private Builder() {
         this.indexesByName = new com.google.common.collect.ImmutableMap.Builder();
         this.indexesById = new com.google.common.collect.ImmutableMap.Builder();
      }

      public Indexes build() {
         return new Indexes(this);
      }

      public Indexes.Builder add(IndexMetadata index) {
         this.indexesByName.put(index.name, index);
         this.indexesById.put(index.id, index);
         return this;
      }

      public Indexes.Builder add(IndexMetadata... indexes) {
         IndexMetadata[] var2 = indexes;
         int var3 = indexes.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            IndexMetadata index = var2[var4];
            this.add(index);
         }

         return this;
      }

      public Indexes.Builder add(Iterable<IndexMetadata> indexes) {
         indexes.forEach(this::add);
         return this;
      }
   }
}
