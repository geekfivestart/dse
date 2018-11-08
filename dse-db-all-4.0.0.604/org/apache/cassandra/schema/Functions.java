package org.apache.cassandra.schema;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

public final class Functions implements Iterable<Function> {
   private final ImmutableMultimap<FunctionName, Function> functions;

   private Functions(Functions.Builder builder) {
      this.functions = builder.functions.build();
   }

   public static Functions.Builder builder() {
      return new Functions.Builder();
   }

   public static Functions none() {
      return builder().build();
   }

   public static Functions of(Function... funs) {
      return builder().add(funs).build();
   }

   public Iterator<Function> iterator() {
      return this.functions.values().iterator();
   }

   public Stream<Function> stream() {
      return this.functions.values().stream();
   }

   public Stream<UDFunction> udfs() {
      return this.stream().filter((f) -> {
         return f instanceof UDFunction;
      }).map((f) -> {
         return (UDFunction)f;
      });
   }

   public Stream<UDAggregate> udas() {
      return this.stream().filter((f) -> {
         return f instanceof UDAggregate;
      }).map((f) -> {
         return (UDAggregate)f;
      });
   }

   MapDifference<Pair<FunctionName, List<String>>, UDFunction> udfsDiff(Functions other) {
      Map<Pair<FunctionName, List<String>>, UDFunction> before = new HashMap();
      this.udfs().forEach((f) -> {
         UDFunction var10000 = (UDFunction)before.put(Pair.create(f.name(), f.argumentsList()), f);
      });
      Map<Pair<FunctionName, List<String>>, UDFunction> after = new HashMap();
      other.udfs().forEach((f) -> {
         UDFunction var10000 = (UDFunction)after.put(Pair.create(f.name(), f.argumentsList()), f);
      });
      return Maps.difference(before, after);
   }

   MapDifference<Pair<FunctionName, List<String>>, UDAggregate> udasDiff(Functions other) {
      Map<Pair<FunctionName, List<String>>, UDAggregate> before = new HashMap();
      this.udas().forEach((f) -> {
         UDAggregate var10000 = (UDAggregate)before.put(Pair.create(f.name(), f.argumentsList()), f);
      });
      Map<Pair<FunctionName, List<String>>, UDAggregate> after = new HashMap();
      other.udas().forEach((f) -> {
         UDAggregate var10000 = (UDAggregate)after.put(Pair.create(f.name(), f.argumentsList()), f);
      });
      return Maps.difference(before, after);
   }

   public Collection<UDAggregate> aggregatesUsingFunction(Function function) {
      return (Collection)this.udas().filter((uda) -> {
         return uda.hasReferenceTo(function);
      }).collect(Collectors.toList());
   }

   public Collection<Function> get(FunctionName name) {
      return this.functions.get(name);
   }

   public Optional<Function> find(FunctionName name, List<AbstractType<?>> argTypes) {
      return this.get(name).stream().filter((fun) -> {
         return typesMatch(fun.argTypes(), argTypes);
      }).findAny();
   }

   public static boolean typesMatch(AbstractType<?> t1, AbstractType<?> t2) {
      return t1.freeze().asCQL3Type().toString().equals(t2.freeze().asCQL3Type().toString());
   }

   public static boolean typesMatch(List<AbstractType<?>> t1, List<AbstractType<?>> t2) {
      if(t1.size() != t2.size()) {
         return false;
      } else {
         for(int i = 0; i < t1.size(); ++i) {
            if(!typesMatch((AbstractType)t1.get(i), (AbstractType)t2.get(i))) {
               return false;
            }
         }

         return true;
      }
   }

   public static int typeHashCode(AbstractType<?> t) {
      return t.asCQL3Type().toString().hashCode();
   }

   public static int typeHashCode(List<AbstractType<?>> types) {
      int h = 0;

      AbstractType type;
      for(Iterator var2 = types.iterator(); var2.hasNext(); h = h * 31 + typeHashCode(type)) {
         type = (AbstractType)var2.next();
      }

      return h;
   }

   public Functions with(Function fun) {
      if(this.find(fun.name(), fun.argTypes()).isPresent()) {
         throw new IllegalStateException(String.format("Function %s already exists", new Object[]{fun.name()}));
      } else {
         return builder().add((Iterable)this).add(fun).build();
      }
   }

   public Functions without(FunctionName name, List<AbstractType<?>> argTypes) {
      Function fun = (Function)this.find(name, argTypes).orElseThrow(() -> {
         return new IllegalStateException(String.format("Function %s doesn't exists", new Object[]{name}));
      });
      return builder().add(Iterables.filter(this, (f) -> {
         return f != fun;
      })).build();
   }

   public boolean equals(Object o) {
      return this == o || o instanceof Functions && this.functions.equals(((Functions)o).functions);
   }

   public int hashCode() {
      return this.functions.hashCode();
   }

   public String toString() {
      return this.functions.values().toString();
   }

   public static final class Builder {
      final com.google.common.collect.ImmutableMultimap.Builder<FunctionName, Function> functions;

      private Builder() {
         this.functions = new com.google.common.collect.ImmutableMultimap.Builder();
         this.functions.orderValuesBy(Comparator.comparingInt(Object::hashCode));
      }

      public Functions build() {
         return new Functions(this);
      }

      public Functions.Builder add(Function fun) {
         this.functions.put(fun.name(), fun);
         return this;
      }

      public Functions.Builder add(Function... funs) {
         Function[] var2 = funs;
         int var3 = funs.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Function fun = var2[var4];
            this.add(fun);
         }

         return this;
      }

      public Functions.Builder add(Iterable<? extends Function> funs) {
         funs.forEach(this::add);
         return this;
      }
   }
}
