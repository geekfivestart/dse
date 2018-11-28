package com.datastax.bdp.util.genericql;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.ResultSet.ResultMetadata;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;

public class ObjectSerializer<T> {
   public final ImmutableSortedMap<String, ObjectSerializer<T>.FieldSerializer> serializers;

   public ObjectSerializer(Class<T> clazz, Type genericType) {
      this.serializers = GenericSerializer.simpleType(genericType)?ImmutableSortedMap.of("result", new ObjectSerializer.FieldSerializer(GenericSerializer.getType(genericType), (x) -> {
         return x;
      })):ImmutableSortedMap.copyOf((Map)Arrays.stream(clazz.getFields()).collect(Collectors.toMap((field) -> {
         return field.getName();
      }, (field) -> {
         return new ObjectSerializer.FieldSerializer(GenericSerializer.getType(field.getType()), field);
      })));
   }

   public ObjectSerializer(Class<T> clazz) {
      this(clazz, clazz);
   }

   public ResultSet toResultSet(T obj, String ksName, String cfName) {
      return new ResultSet(new ResultMetadata((List)this.serializers.entrySet().stream().map((e) -> {
         return new ColumnSpecification(ksName, cfName, new ColumnIdentifier((String)e.getKey(), true), ((ObjectSerializer.FieldSerializer)e.getValue()).type);
      }).collect(Collectors.toList())), Lists.newArrayList(new List[]{this.toByteBufferList(obj)}));
   }

   public ResultSet toMultiRowResultSet(Collection<T> obj, String ksName, String cfName) {
      return new ResultSet(new ResultMetadata((List)this.serializers.entrySet().stream().map((e) -> {
         return new ColumnSpecification(ksName, cfName, new ColumnIdentifier((String)e.getKey(), true), ((ObjectSerializer.FieldSerializer)e.getValue()).type);
      }).collect(Collectors.toList())), (List)obj.stream().map(this::toByteBufferList).collect(Collectors.toList()));
   }

   public List<ByteBuffer> toByteBufferList(T obj) {
      return (List)this.serializers.values().stream().map((fs) -> {
         return fs.serializeField(obj);
      }).collect(Collectors.toList());
   }

   public ByteBuffer toByteBuffer(T obj) {
      return TupleType.buildValue((ByteBuffer[])this.serializers.values().stream().map((fs) -> {
         return fs.serializeField(obj);
      }).toArray((x$0) -> {
         return new ByteBuffer[x$0];
      }));
   }

   public class FieldSerializer {
      public final AbstractType type;
      public final Function<T, Object> accessor;

      FieldSerializer(AbstractType type, Function<T, Object> accessor) {
         this.type = type;
         this.accessor = accessor;
      }

      FieldSerializer(AbstractType type, Field field) {
         field.setAccessible(true);
         this.type = type;
         this.accessor = (obj) -> {
            try {
               return field.get(obj);
            } catch (IllegalAccessException var3) {
               throw new AssertionError("Should not happen as we set the field to accessible.");
            }
         };
      }

      ByteBuffer serializeField(T obj) {
         Object value = this.accessor.apply(obj);
         return value == null?null:this.type.getSerializer().serialize(this.accessor.apply(obj));
      }
   }
}
