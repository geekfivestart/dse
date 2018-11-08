package org.apache.cassandra.schema;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class Views implements Iterable<ViewMetadata> {
   private final ImmutableMap<String, ViewMetadata> views;

   private Views(Views.Builder builder) {
      this.views = builder.views.build();
   }

   public static Views.Builder builder() {
      return new Views.Builder();
   }

   public static Views none() {
      return builder().build();
   }

   public Iterator<ViewMetadata> iterator() {
      return this.views.values().iterator();
   }

   public Iterable<TableMetadata> metadatas() {
      return Iterables.transform(this.views.values(), (view) -> {
         return view.metadata;
      });
   }

   public int size() {
      return this.views.size();
   }

   public boolean isEmpty() {
      return this.views.isEmpty();
   }

   public Iterable<ViewMetadata> forTable(UUID tableId) {
      return Iterables.filter(this, (v) -> {
         return v.baseTableId.asUUID().equals(tableId);
      });
   }

   public Optional<ViewMetadata> get(String name) {
      return Optional.ofNullable(this.views.get(name));
   }

   @Nullable
   public ViewMetadata getNullable(String name) {
      return (ViewMetadata)this.views.get(name);
   }

   public Views with(ViewMetadata view) {
      if(this.get(view.name).isPresent()) {
         throw new IllegalStateException(String.format("Materialized View %s already exists", new Object[]{view.name}));
      } else {
         return builder().add((Iterable)this).add(view).build();
      }
   }

   public Views withSwapped(ViewMetadata view) {
      return this.without(view.name).with(view);
   }

   public Views without(String name) {
      ViewMetadata materializedView = (ViewMetadata)this.get(name).orElseThrow(() -> {
         return new IllegalStateException(String.format("Materialized View %s doesn't exists", new Object[]{name}));
      });
      return builder().add(Iterables.filter(this, (v) -> {
         return v != materializedView;
      })).build();
   }

   MapDifference<TableId, ViewMetadata> diff(Views other) {
      Map<TableId, ViewMetadata> thisViews = new HashMap();
      this.forEach((v) -> {
         ViewMetadata var10000 = (ViewMetadata)thisViews.put(v.metadata.id, v);
      });
      Map<TableId, ViewMetadata> otherViews = new HashMap();
      other.forEach((v) -> {
         ViewMetadata var10000 = (ViewMetadata)otherViews.put(v.metadata.id, v);
      });
      return Maps.difference(thisViews, otherViews);
   }

   public boolean equals(Object o) {
      return this == o || o instanceof Views && this.views.equals(((Views)o).views);
   }

   public int hashCode() {
      return this.views.hashCode();
   }

   public String toString() {
      return this.views.values().toString();
   }

   public static final class Builder {
      final com.google.common.collect.ImmutableMap.Builder<String, ViewMetadata> views;

      private Builder() {
         this.views = new com.google.common.collect.ImmutableMap.Builder();
      }

      public Views build() {
         return new Views(this);
      }

      public Views.Builder add(ViewMetadata view) {
         this.views.put(view.name, view);
         return this;
      }

      public Views.Builder add(Iterable<ViewMetadata> views) {
         views.forEach(this::add);
         return this;
      }
   }
}
