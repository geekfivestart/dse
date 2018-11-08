package org.apache.cassandra.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class Triggers implements Iterable<TriggerMetadata> {
   private final ImmutableMap<String, TriggerMetadata> triggers;

   private Triggers(Triggers.Builder builder) {
      this.triggers = builder.triggers.build();
   }

   public static Triggers.Builder builder() {
      return new Triggers.Builder();
   }

   public static Triggers none() {
      return builder().build();
   }

   public static Triggers of(TriggerMetadata... triggers) {
      return builder().add(triggers).build();
   }

   public static Triggers of(Iterable<TriggerMetadata> triggers) {
      return builder().add(triggers).build();
   }

   public Iterator<TriggerMetadata> iterator() {
      return this.triggers.values().iterator();
   }

   public int size() {
      return this.triggers.size();
   }

   public boolean isEmpty() {
      return this.triggers.isEmpty();
   }

   public Optional<TriggerMetadata> get(String name) {
      return Optional.ofNullable(this.triggers.get(name));
   }

   public Triggers with(TriggerMetadata trigger) {
      if(this.get(trigger.name).isPresent()) {
         throw new IllegalStateException(String.format("Trigger %s already exists", new Object[]{trigger.name}));
      } else {
         return builder().add((Iterable)this).add(trigger).build();
      }
   }

   public Triggers without(String name) {
      TriggerMetadata trigger = (TriggerMetadata)this.get(name).orElseThrow(() -> {
         return new IllegalStateException(String.format("Trigger %s doesn't exists", new Object[]{name}));
      });
      return builder().add(Iterables.filter(this, (t) -> {
         return t != trigger;
      })).build();
   }

   public boolean equals(Object o) {
      return this == o || o instanceof Triggers && this.triggers.equals(((Triggers)o).triggers);
   }

   public int hashCode() {
      return this.triggers.hashCode();
   }

   public String toString() {
      return this.triggers.values().toString();
   }

   public static final class Builder {
      final com.google.common.collect.ImmutableMap.Builder<String, TriggerMetadata> triggers;

      private Builder() {
         this.triggers = new com.google.common.collect.ImmutableMap.Builder();
      }

      public Triggers build() {
         return new Triggers(this);
      }

      public Triggers.Builder add(TriggerMetadata trigger) {
         this.triggers.put(trigger.name, trigger);
         return this;
      }

      public Triggers.Builder add(TriggerMetadata... triggers) {
         TriggerMetadata[] var2 = triggers;
         int var3 = triggers.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            TriggerMetadata trigger = var2[var4];
            this.add(trigger);
         }

         return this;
      }

      public Triggers.Builder add(Iterable<TriggerMetadata> triggers) {
         triggers.forEach(this::add);
         return this;
      }
   }
}
