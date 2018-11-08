package org.apache.cassandra.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class Types implements Iterable<UserType> {
   private static final Types NONE = new Types(ImmutableMap.of());
   private final Map<ByteBuffer, UserType> types;

   private Types(Types.Builder builder) {
      this.types = builder.types.build();
   }

   private Types(Map<ByteBuffer, UserType> types) {
      this.types = types;
   }

   public static Types.Builder builder() {
      return new Types.Builder();
   }

   public static Types.RawBuilder rawBuilder(String keyspace) {
      return new Types.RawBuilder(keyspace);
   }

   public static Types none() {
      return NONE;
   }

   public static Types of(UserType... types) {
      return types.length == 0?none():builder().add(types).build();
   }

   public static Types of(Collection<UserType> types) {
      return types.isEmpty()?none():builder().add((Iterable)types).build();
   }

   public Iterator<UserType> iterator() {
      return this.types.values().iterator();
   }

   public Optional<UserType> get(ByteBuffer name) {
      return Optional.ofNullable(this.types.get(name));
   }

   @Nullable
   public UserType getNullable(ByteBuffer name) {
      return (UserType)this.types.get(name);
   }

   public Types with(UserType type) {
      if(this.get(type.name).isPresent()) {
         throw new IllegalStateException(String.format("Type %s already exists", new Object[]{type.name}));
      } else {
         return builder().add((Iterable)this).add(type).build();
      }
   }

   public Types without(ByteBuffer name) {
      UserType type = (UserType)this.get(name).orElseThrow(() -> {
         return new IllegalStateException(String.format("Type %s doesn't exists", new Object[]{name}));
      });
      return builder().add(Iterables.filter(this, (t) -> {
         return t != type;
      })).build();
   }

   MapDifference<ByteBuffer, UserType> diff(Types other) {
      return Maps.difference(this.types, other.types);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof Types)) {
         return false;
      } else {
         Types other = (Types)o;
         if(this.types.size() != other.types.size()) {
            return false;
         } else {
            Iterator<Entry<ByteBuffer, UserType>> thisIter = this.types.entrySet().iterator();
            Iterator otherIter = other.types.entrySet().iterator();

            Entry thisNext;
            Entry otherNext;
            do {
               if(!thisIter.hasNext()) {
                  return true;
               }

               thisNext = (Entry)thisIter.next();
               otherNext = (Entry)otherIter.next();
               if(!((ByteBuffer)thisNext.getKey()).equals(otherNext.getKey())) {
                  return false;
               }
            } while(((UserType)thisNext.getValue()).equals(otherNext.getValue(), true));

            return false;
         }
      }
   }

   public int hashCode() {
      return this.types.hashCode();
   }

   public String toString() {
      return this.types.values().toString();
   }

   public static final class RawBuilder {
      final String keyspace;
      final List<Types.RawBuilder.RawUDT> definitions;

      private RawBuilder(String keyspace) {
         this.keyspace = keyspace;
         this.definitions = new ArrayList();
      }

      public Types build() {
         if(this.definitions.isEmpty()) {
            return Types.NONE;
         } else {
            Map<Types.RawBuilder.RawUDT, Integer> vertices = Maps.newHashMapWithExpectedSize(this.definitions.size());
            Iterator var2 = this.definitions.iterator();

            while(var2.hasNext()) {
               Types.RawBuilder.RawUDT udt = (Types.RawBuilder.RawUDT)var2.next();
               vertices.put(udt, Integer.valueOf(0));
            }

            Multimap<Types.RawBuilder.RawUDT, Types.RawBuilder.RawUDT> adjacencyList = HashMultimap.create();
            Iterator var9 = this.definitions.iterator();

            while(var9.hasNext()) {
               Types.RawBuilder.RawUDT udt1 = (Types.RawBuilder.RawUDT)var9.next();
               Iterator var5 = this.definitions.iterator();

               while(var5.hasNext()) {
                  Types.RawBuilder.RawUDT udt2 = (Types.RawBuilder.RawUDT)var5.next();
                  if(udt1 != udt2 && udt1.referencesUserType(udt2)) {
                     adjacencyList.put(udt2, udt1);
                  }
               }
            }

            adjacencyList.values().forEach((vertex) -> {
               Integer var10000 = (Integer)vertices.put(vertex, Integer.valueOf(((Integer)vertices.get(vertex)).intValue() + 1));
            });
            Queue<Types.RawBuilder.RawUDT> resolvableTypes = new LinkedList();
            Iterator var11 = vertices.entrySet().iterator();

            while(var11.hasNext()) {
               Entry<Types.RawBuilder.RawUDT, Integer> entry = (Entry)var11.next();
               if(((Integer)entry.getValue()).intValue() == 0) {
                  resolvableTypes.add(entry.getKey());
               }
            }

            Types types = new Types(new HashMap());

            while(!resolvableTypes.isEmpty()) {
               Types.RawBuilder.RawUDT vertex = (Types.RawBuilder.RawUDT)resolvableTypes.remove();
               Iterator var15 = adjacencyList.get(vertex).iterator();

               while(var15.hasNext()) {
                  Types.RawBuilder.RawUDT dependentType = (Types.RawBuilder.RawUDT)var15.next();
                  if(((Integer)vertices.replace(dependentType, Integer.valueOf(((Integer)vertices.get(dependentType)).intValue() - 1))).intValue() == 1) {
                     resolvableTypes.add(dependentType);
                  }
               }

               UserType udt = vertex.prepare(this.keyspace, types);
               types.types.put(udt.name, udt);
            }

            if(types.types.size() != this.definitions.size()) {
               throw new ConfigurationException(String.format("Cannot resolve UDTs for keyspace %s: some types are missing", new Object[]{this.keyspace}));
            } else {
               return Types.builder().add((Iterable)types).build();
            }
         }
      }

      public void add(String name, List<String> fieldNames, List<String> fieldTypes) {
         List<CQL3Type.Raw> rawFieldTypes = (List)fieldTypes.stream().map(CQLTypeParser::parseRaw).collect(Collectors.toList());
         this.definitions.add(new Types.RawBuilder.RawUDT(name, fieldNames, rawFieldTypes));
      }

      private static final class RawUDT {
         final String name;
         final List<String> fieldNames;
         final List<CQL3Type.Raw> fieldTypes;

         RawUDT(String name, List<String> fieldNames, List<CQL3Type.Raw> fieldTypes) {
            this.name = name;
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
         }

         boolean referencesUserType(Types.RawBuilder.RawUDT other) {
            return this.fieldTypes.stream().anyMatch((t) -> {
               return t.referencesUserType(other.name);
            });
         }

         UserType prepare(String keyspace, Types types) {
            List<FieldIdentifier> preparedFieldNames = (List)this.fieldNames.stream().map((t) -> {
               return FieldIdentifier.forInternalString(t);
            }).collect(Collectors.toList());
            List<AbstractType<?>> preparedFieldTypes = (List)this.fieldTypes.stream().map((t) -> {
               return t.prepareInternal(keyspace, types).getType();
            }).collect(Collectors.toList());
            return new UserType(keyspace, ByteBufferUtil.bytes(this.name), preparedFieldNames, preparedFieldTypes, true);
         }

         public int hashCode() {
            return this.name.hashCode();
         }

         public boolean equals(Object other) {
            return this.name.equals(((Types.RawBuilder.RawUDT)other).name);
         }
      }
   }

   public static final class Builder {
      final com.google.common.collect.ImmutableSortedMap.Builder<ByteBuffer, UserType> types;

      private Builder() {
         this.types = ImmutableSortedMap.naturalOrder();
      }

      public Types build() {
         return new Types(this);
      }

      public Types.Builder add(UserType type) {
         assert type.isMultiCell();

         this.types.put(type.name, type);
         return this;
      }

      public Types.Builder add(UserType... types) {
         UserType[] var2 = types;
         int var3 = types.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            UserType type = var2[var4];
            this.add(type);
         }

         return this;
      }

      public Types.Builder add(Iterable<UserType> types) {
         types.forEach(this::add);
         return this;
      }
   }
}
