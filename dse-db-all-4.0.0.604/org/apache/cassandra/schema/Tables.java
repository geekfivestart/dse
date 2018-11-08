package org.apache.cassandra.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.cassandra.index.internal.CassandraIndex;

public final class Tables implements Iterable<TableMetadata> {
   private final ImmutableMap<String, TableMetadata> tables;
   private final ImmutableMap<String, TableMetadata> indexTables;

   private Tables(Tables.Builder builder) {
      this.tables = builder.tables.build();
      this.indexTables = builder.indexTables.build();
   }

   public static Tables.Builder builder() {
      return new Tables.Builder();
   }

   public static Tables none() {
      return builder().build();
   }

   public static Tables of(TableMetadata... tables) {
      return builder().add(tables).build();
   }

   public static Tables of(Iterable<TableMetadata> tables) {
      return builder().add(tables).build();
   }

   public Iterator<TableMetadata> iterator() {
      return this.tables.values().iterator();
   }

   ImmutableMap<String, TableMetadata> indexTables() {
      return this.indexTables;
   }

   public int size() {
      return this.tables.size();
   }

   public Set<String> tableNames() {
      return this.tables.keySet();
   }

   public Optional<TableMetadata> get(String name) {
      return Optional.ofNullable(this.tables.get(name));
   }

   @Nullable
   public TableMetadata getNullable(String name) {
      return (TableMetadata)this.tables.get(name);
   }

   @Nullable
   public TableMetadata getIndexTableNullable(String name) {
      return (TableMetadata)this.indexTables.get(name);
   }

   public Tables with(TableMetadata table) {
      if(this.get(table.name).isPresent()) {
         throw new IllegalStateException(String.format("Table %s already exists", new Object[]{table.name}));
      } else {
         return builder().add((Iterable)this).add(table).build();
      }
   }

   public Tables withSwapped(TableMetadata table) {
      return this.without(table.name).with(table);
   }

   public Tables without(String name) {
      TableMetadata table = (TableMetadata)this.get(name).orElseThrow(() -> {
         return new IllegalStateException(String.format("Table %s doesn't exists", new Object[]{name}));
      });
      return builder().add(Iterables.filter(this, (t) -> {
         return t != table;
      })).build();
   }

   MapDifference<TableId, TableMetadata> diff(Tables other) {
      Map<TableId, TableMetadata> thisTables = new HashMap();
      this.forEach((t) -> {
         TableMetadata var10000 = (TableMetadata)thisTables.put(t.id, t);
      });
      Map<TableId, TableMetadata> otherTables = new HashMap();
      other.forEach((t) -> {
         TableMetadata var10000 = (TableMetadata)otherTables.put(t.id, t);
      });
      return Maps.difference(thisTables, otherTables);
   }

   MapDifference<String, TableMetadata> indexesDiff(Tables other) {
      Map<String, TableMetadata> thisIndexTables = new HashMap();
      this.indexTables.values().forEach((t) -> {
         TableMetadata var10000 = (TableMetadata)thisIndexTables.put(t.indexName().get(), t);
      });
      Map<String, TableMetadata> otherIndexTables = new HashMap();
      other.indexTables.values().forEach((t) -> {
         TableMetadata var10000 = (TableMetadata)otherIndexTables.put(t.indexName().get(), t);
      });
      return Maps.difference(thisIndexTables, otherIndexTables);
   }

   public boolean equals(Object o) {
      return this == o || o instanceof Tables && this.tables.equals(((Tables)o).tables);
   }

   public int hashCode() {
      return this.tables.hashCode();
   }

   public String toString() {
      return this.tables.values().toString();
   }

   public static final class Builder {
      final com.google.common.collect.ImmutableMap.Builder<String, TableMetadata> tables;
      final com.google.common.collect.ImmutableMap.Builder<String, TableMetadata> indexTables;

      private Builder() {
         this.tables = new com.google.common.collect.ImmutableMap.Builder();
         this.indexTables = new com.google.common.collect.ImmutableMap.Builder();
      }

      public Tables build() {
         return new Tables(this);
      }

      public Tables.Builder add(TableMetadata table) {
         this.tables.put(table.name, table);
         table.indexes.stream().filter((i) -> {
            return !i.isCustom();
         }).map((i) -> {
            return CassandraIndex.indexCfsMetadata(table, i);
         }).forEach((i) -> {
            this.indexTables.put(i.indexName().get(), i);
         });
         return this;
      }

      public Tables.Builder add(TableMetadata... tables) {
         TableMetadata[] var2 = tables;
         int var3 = tables.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            TableMetadata table = var2[var4];
            this.add(table);
         }

         return this;
      }

      public Tables.Builder add(Iterable<TableMetadata> tables) {
         tables.forEach(this::add);
         return this;
      }
   }
}
