package org.apache.cassandra.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public final class Keyspaces implements Iterable<KeyspaceMetadata> {
   private final ImmutableMap<String, KeyspaceMetadata> keyspaces;
   private final ImmutableMap<TableId, TableMetadata> tables;

   private Keyspaces(Keyspaces.Builder builder) {
      this.keyspaces = builder.keyspaces.build();
      this.tables = builder.tables.build();
   }

   public static Keyspaces.Builder builder() {
      return new Keyspaces.Builder();
   }

   public static Keyspaces none() {
      return builder().build();
   }

   public static Keyspaces of(KeyspaceMetadata... keyspaces) {
      return builder().add(keyspaces).build();
   }

   public Iterator<KeyspaceMetadata> iterator() {
      return this.keyspaces.values().iterator();
   }

   public Stream<KeyspaceMetadata> stream() {
      return this.keyspaces.values().stream();
   }

   public Set<String> names() {
      return this.keyspaces.keySet();
   }

   @Nullable
   public KeyspaceMetadata getNullable(String name) {
      return (KeyspaceMetadata)this.keyspaces.get(name);
   }

   @Nullable
   public TableMetadata getTableOrViewNullable(TableId id) {
      return (TableMetadata)this.tables.get(id);
   }

   public Keyspaces filter(Predicate<KeyspaceMetadata> predicate) {
      Keyspaces.Builder builder = builder();
      this.stream().filter(predicate).forEach(builder::add);
      return builder.build();
   }

   public Keyspaces without(String name) {
      KeyspaceMetadata keyspace = this.getNullable(name);
      if(keyspace == null) {
         throw new IllegalStateException(String.format("Keyspace %s doesn't exists", new Object[]{name}));
      } else {
         return builder().add((Iterable)this.filter((k) -> {
            return k != keyspace;
         })).build();
      }
   }

   public Keyspaces withAddedOrUpdated(KeyspaceMetadata keyspace) {
      return builder().add((Iterable)this.filter((k) -> {
         return !k.name.equals(keyspace.name);
      })).add(keyspace).build();
   }

   MapDifference<String, KeyspaceMetadata> diff(Keyspaces other) {
      return Maps.difference(this.keyspaces, other.keyspaces);
   }

   public boolean equals(Object o) {
      return this == o || o instanceof Keyspaces && this.keyspaces.equals(((Keyspaces)o).keyspaces);
   }

   public int hashCode() {
      return this.keyspaces.hashCode();
   }

   public String toString() {
      return this.keyspaces.values().toString();
   }

   public static final class Builder {
      private final com.google.common.collect.ImmutableMap.Builder<String, KeyspaceMetadata> keyspaces;
      private final com.google.common.collect.ImmutableMap.Builder<TableId, TableMetadata> tables;

      private Builder() {
         this.keyspaces = new com.google.common.collect.ImmutableMap.Builder();
         this.tables = new com.google.common.collect.ImmutableMap.Builder();
      }

      public Keyspaces build() {
         return new Keyspaces(this);
      }

      public Keyspaces.Builder add(KeyspaceMetadata keyspace) {
         this.keyspaces.put(keyspace.name, keyspace);
         keyspace.tables.forEach((t) -> {
            this.tables.put(t.id, t);
         });
         keyspace.views.forEach((v) -> {
            this.tables.put(v.metadata.id, v.metadata);
         });
         return this;
      }

      public Keyspaces.Builder add(KeyspaceMetadata... keyspaces) {
         KeyspaceMetadata[] var2 = keyspaces;
         int var3 = keyspaces.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            KeyspaceMetadata keyspace = var2[var4];
            this.add(keyspace);
         }

         return this;
      }

      public Keyspaces.Builder add(Iterable<KeyspaceMetadata> keyspaces) {
         keyspaces.forEach(this::add);
         return this;
      }
   }
}
