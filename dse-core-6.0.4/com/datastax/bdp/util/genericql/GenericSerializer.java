package com.datastax.bdp.util.genericql;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.serializers.TypeSerializer;

public class GenericSerializer {
   private static final ConcurrentHashMap<String, AbstractType> typeMap = new ConcurrentHashMap() {
      {
         this.put("void", EmptyType.instance);
         this.put("boolean", BooleanType.instance);
         this.put("java.lang.Boolean", BooleanType.instance);
         this.put("byte", ByteType.instance);
         this.put("java.lang.Byte", ByteType.instance);
         this.put("int", Int32Type.instance);
         this.put("java.lang.Integer", Int32Type.instance);
         this.put("long", LongType.instance);
         this.put("java.lang.Long", LongType.instance);
         this.put("float", FloatType.instance);
         this.put("java.lang.Float", FloatType.instance);
         this.put("double", DoubleType.instance);
         this.put("java.lang.Double", DoubleType.instance);
         this.put("java.lang.String", UTF8Type.instance);
         this.put("java.net.InetAddress", InetAddressType.instance);
         this.put("java.util.Date", DateType.instance);
         this.put("java.nio.ByteBuffer", BytesType.instance);
         this.put("java.util.UUID", UUIDType.instance);
      }
   };

   public GenericSerializer() {
   }

   public static void registerType(String className, AbstractType<?> type) {
      if(typeMap.putIfAbsent(className, type) != null) {
         throw new IllegalStateException("The type " + className + " is already registered.");
      }
   }

   public static TypeSerializer getSerializer(Type type) {
      return getTypeOrException(type).getSerializer();
   }

   public static AbstractType getTypeOrException(Type type) {
      AbstractType ctype = getType(type);
      if(ctype == null) {
         throw new AssertionError(String.format("Add type '%s' to GenericSerializer", new Object[]{type.getTypeName()}));
      } else {
         return ctype;
      }
   }

   public static boolean simpleType(Type type) {
      return getType(type) != null;
   }

   public static AbstractType getType(Type type) {
      assert type != null;

      String strType = type.getTypeName();
      if(!typeMap.containsKey(strType)) {
         if(!(type instanceof ParameterizedType)) {
            return null;
         }

         ParameterizedType ptype = (ParameterizedType)type;
         if(ptype.getRawType().getTypeName().equals("java.util.List")) {
            assert ptype.getActualTypeArguments().length == 1;

            typeMap.putIfAbsent(strType, ListType.getInstance(getType(ptype.getActualTypeArguments()[0]), false));
         } else if(ptype.getRawType().getTypeName().equals("java.util.Set")) {
            assert ptype.getActualTypeArguments().length == 1;

            typeMap.putIfAbsent(strType, SetType.getInstance(getType(ptype.getActualTypeArguments()[0]), false));
         } else {
            if(!ptype.getRawType().getTypeName().equals("java.util.Map")) {
               throw new AssertionError("Don't know how to serialize generic type '" + ptype.getRawType().getTypeName() + "'");
            }

            assert ptype.getActualTypeArguments().length == 2;

            typeMap.putIfAbsent(strType, MapType.getInstance(getType(ptype.getActualTypeArguments()[0]), getType(ptype.getActualTypeArguments()[1]), false));
         }
      }

      return (AbstractType)typeMap.get(strType);
   }
}
