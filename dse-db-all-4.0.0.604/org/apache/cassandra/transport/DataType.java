package org.apache.cassandra.transport;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.utils.Pair;

public enum DataType {
   CUSTOM(0, (AbstractType)null, ProtocolVersion.V1),
   ASCII(1, AsciiType.instance, ProtocolVersion.V1),
   BIGINT(2, LongType.instance, ProtocolVersion.V1),
   BLOB(3, BytesType.instance, ProtocolVersion.V1),
   BOOLEAN(4, BooleanType.instance, ProtocolVersion.V1),
   COUNTER(5, CounterColumnType.instance, ProtocolVersion.V1),
   DECIMAL(6, DecimalType.instance, ProtocolVersion.V1),
   DOUBLE(7, DoubleType.instance, ProtocolVersion.V1),
   FLOAT(8, FloatType.instance, ProtocolVersion.V1),
   INT(9, Int32Type.instance, ProtocolVersion.V1),
   TEXT(10, UTF8Type.instance, ProtocolVersion.V1),
   TIMESTAMP(11, TimestampType.instance, ProtocolVersion.V1),
   UUID(12, UUIDType.instance, ProtocolVersion.V1),
   VARCHAR(13, UTF8Type.instance, ProtocolVersion.V1),
   VARINT(14, IntegerType.instance, ProtocolVersion.V1),
   TIMEUUID(15, TimeUUIDType.instance, ProtocolVersion.V1),
   INET(16, InetAddressType.instance, ProtocolVersion.V1),
   DATE(17, SimpleDateType.instance, ProtocolVersion.V4),
   TIME(18, TimeType.instance, ProtocolVersion.V4),
   SMALLINT(19, ShortType.instance, ProtocolVersion.V4),
   BYTE(20, ByteType.instance, ProtocolVersion.V4),
   DURATION(21, DurationType.instance, ProtocolVersion.V5),
   LIST(32, (AbstractType)null, ProtocolVersion.V1),
   MAP(33, (AbstractType)null, ProtocolVersion.V1),
   SET(34, (AbstractType)null, ProtocolVersion.V1),
   UDT(48, (AbstractType)null, ProtocolVersion.V3),
   TUPLE(49, (AbstractType)null, ProtocolVersion.V3);

   public static final DataType.Codec codec = new DataType.Codec();
   private final int id;
   private final ProtocolVersion protocolVersion;
   private final AbstractType type;
   private final Pair<DataType, Object> pair;
   private static final Map<AbstractType, DataType> dataTypeMap = new HashMap();

   private DataType(int id, AbstractType type, ProtocolVersion protocolVersion) {
      this.id = id;
      this.type = type;
      this.protocolVersion = protocolVersion;
      this.pair = Pair.create(this, (Object)null);
   }

   public int getId(ProtocolVersion version) {
      return version.isSmallerThan(this.protocolVersion)?CUSTOM.getId(version):this.id;
   }

   public Object readValue(ByteBuf cb, ProtocolVersion version) {
      switch (this) {
         case CUSTOM: {
            return CBUtil.readString(cb);
         }
         case LIST: {
            return DataType.toType(codec.decodeOne(cb, version));
         }
         case SET: {
            return DataType.toType(codec.decodeOne(cb, version));
         }
         case MAP: {
            ArrayList<AbstractType> l = new ArrayList<AbstractType>(2);
            l.add(DataType.toType(codec.decodeOne(cb, version)));
            l.add(DataType.toType(codec.decodeOne(cb, version)));
            return l;
         }
         case UDT: {
            String ks = CBUtil.readString(cb);
            ByteBuffer name = UTF8Type.instance.decompose(CBUtil.readString(cb));
            int n = cb.readUnsignedShort();
            ArrayList<FieldIdentifier> fieldNames = new ArrayList<FieldIdentifier>(n);
            ArrayList fieldTypes = new ArrayList(n);
            for (int i = 0; i < n; ++i) {
               fieldNames.add(FieldIdentifier.forInternalString(CBUtil.readString(cb)));
               fieldTypes.add(DataType.toType(codec.decodeOne(cb, version)));
            }
            return new UserType(ks, name, fieldNames, fieldTypes, true);
         }
         case TUPLE: {
            int n = cb.readUnsignedShort();
            ArrayList types = new ArrayList(n);
            for (int i = 0; i < n; ++i) {
               types.add(DataType.toType(codec.decodeOne(cb, version)));
            }
            return new TupleType(types);
         }
      }
      return null;
   }

   public void writeValue(Object value, ByteBuf cb, ProtocolVersion version) {
      if (version.isSmallerThan(this.protocolVersion)) {
         CBUtil.writeString(value.toString(), cb);
         return;
      }
      switch (this) {
         case CUSTOM: {
            assert (value instanceof String);
            CBUtil.writeString((String)value, cb);
            break;
         }
         case LIST: {
            codec.writeOne(DataType.fromType((AbstractType)value, version), cb, version);
            break;
         }
         case SET: {
            codec.writeOne(DataType.fromType((AbstractType)value, version), cb, version);
            break;
         }
         case MAP: {
            List l = (List)value;
            codec.writeOne(DataType.fromType((AbstractType)l.get(0), version), cb, version);
            codec.writeOne(DataType.fromType((AbstractType)l.get(1), version), cb, version);
            break;
         }
         case UDT: {
            UserType udt = (UserType)value;
            CBUtil.writeString(udt.keyspace, cb);
            CBUtil.writeString((String)UTF8Type.instance.compose(udt.name), cb);
            cb.writeShort(udt.size());
            for (int i = 0; i < udt.size(); ++i) {
               CBUtil.writeString(udt.fieldName(i).toString(), cb);
               codec.writeOne(DataType.fromType(udt.fieldType(i), version), cb, version);
            }
            break;
         }
         case TUPLE: {
            TupleType tt = (TupleType)value;
            cb.writeShort(tt.size());
            for (int i = 0; i < tt.size(); ++i) {
               codec.writeOne(DataType.fromType(tt.type(i), version), cb, version);
            }
            break;
         }
      }
   }

   public int serializedValueSize(Object value, ProtocolVersion version) {
      if (version.isSmallerThan(this.protocolVersion)) {
         return CBUtil.sizeOfString(value.toString());
      }
      switch (this) {
         case CUSTOM: {
            return CBUtil.sizeOfString((String)value);
         }
         case LIST:
         case SET: {
            return codec.oneSerializedSize(DataType.fromType((AbstractType)value, version), version);
         }
         case MAP: {
            List l = (List)value;
            int s = 0;
            s += codec.oneSerializedSize(DataType.fromType((AbstractType)l.get(0), version), version);
            return s += codec.oneSerializedSize(DataType.fromType((AbstractType)l.get(1), version), version);
         }
         case UDT: {
            UserType udt = (UserType)value;
            int size = 0;
            size += CBUtil.sizeOfString(udt.keyspace);
            size += CBUtil.sizeOfString((String)UTF8Type.instance.compose(udt.name));
            size += 2;
            for (int i = 0; i < udt.size(); ++i) {
               size += CBUtil.sizeOfString(udt.fieldName(i).toString());
               size += codec.oneSerializedSize(DataType.fromType(udt.fieldType(i), version), version);
            }
            return size;
         }
         case TUPLE: {
            TupleType tt = (TupleType)value;
            int size = 2;
            for (int i = 0; i < tt.size(); ++i) {
               size += codec.oneSerializedSize(DataType.fromType(tt.type(i), version), version);
            }
            return size;
         }
      }
      return 0;
   }


   public static Pair<DataType, Object> fromType(AbstractType type, ProtocolVersion version) {
      if(type instanceof ReversedType) {
         type = ((ReversedType)type).baseType;
      }

      if(type instanceof DateType) {
         type = TimestampType.instance;
      }

      DataType dt = (DataType)dataTypeMap.get(type);
      if(dt == null) {
         if(((AbstractType)type).isCollection()) {
            if(type instanceof ListType) {
               return Pair.create(LIST, ((ListType)type).getElementsType());
            } else if(type instanceof MapType) {
               MapType mt = (MapType)type;
               return Pair.create(MAP, Arrays.asList(new AbstractType[]{mt.getKeysType(), mt.getValuesType()}));
            } else if(type instanceof SetType) {
               return Pair.create(SET, ((SetType)type).getElementsType());
            } else {
               throw new AssertionError();
            }
         } else {
            return type instanceof UserType && version.isGreaterOrEqualTo(UDT.protocolVersion)?Pair.create(UDT, type):(type instanceof TupleType && version.isGreaterOrEqualTo(TUPLE.protocolVersion)?Pair.create(TUPLE, type):Pair.create(CUSTOM, ((AbstractType)type).toString()));
         }
      } else {
         return version.isSmallerThan(dt.protocolVersion)?Pair.create(CUSTOM, ((AbstractType)type).toString()):dt.pair;
      }
   }


   public static AbstractType toType(Pair<DataType, Object> entry) {
      try {
         switch ((DataType)((Object)entry.left)) {
            case CUSTOM: {
               return TypeParser.parse((String)entry.right);
            }
            case LIST: {
               return ListType.getInstance((AbstractType)entry.right, true);
            }
            case SET: {
               return SetType.getInstance((AbstractType)entry.right, true);
            }
            case MAP: {
               List l = (List)entry.right;
               return MapType.getInstance((AbstractType)l.get(0), (AbstractType)l.get(1), true);
            }
            case UDT: {
               return (AbstractType)entry.right;
            }
            case TUPLE: {
               return (AbstractType)entry.right;
            }
         }
         return ((DataType)entry.left).type;
      }
      catch (RequestValidationException e) {
         throw new ProtocolException(e.getMessage());
      }
   }

   @VisibleForTesting
   public ProtocolVersion getProtocolVersion() {
      return this.protocolVersion;
   }

   static {
      DataType[] var0 = values();
      int var1 = var0.length;

      for(int var2 = 0; var2 < var1; ++var2) {
         DataType type = var0[var2];
         if(type.type != null) {
            dataTypeMap.put(type.type, type);
         }
      }

   }

   public static final class Codec {
      private final DataType[] ids;

      public Codec() {
         DataType[] values = DataType.values();
         this.ids = new DataType[this.getMaxId(values) + 1];
         DataType[] var2 = values;
         int var3 = values.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            DataType opt = var2[var4];
            int id = opt.getId(opt.getProtocolVersion());
            DataType existingType = this.ids[id];
            if(existingType != null) {
               throw new IllegalStateException(String.format("Duplicate option id %d", new Object[]{Integer.valueOf(id)}));
            }

            this.ids[id] = opt;
         }

      }

      private int getMaxId(DataType[] values) {
         int maxId = -1;
         DataType[] var3 = values;
         int var4 = values.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            DataType opt = var3[var5];
            maxId = Math.max(maxId, opt.getId(ProtocolVersion.CURRENT));
         }

         return maxId;
      }

      private DataType fromId(int id) {
         DataType opt = this.ids[id];
         if(opt == null) {
            throw new ProtocolException(String.format("Unknown option id %d", new Object[]{Integer.valueOf(id)}));
         } else {
            return opt;
         }
      }

      public Pair<DataType, Object> decodeOne(ByteBuf body, ProtocolVersion version) {
         DataType opt = this.fromId(body.readUnsignedShort());
         Object value = opt.readValue(body, version);
         return Pair.create(opt, value);
      }

      public void writeOne(Pair<DataType, Object> option, ByteBuf dest, ProtocolVersion version) {
         DataType opt = (DataType)option.left;
         Object obj = option.right;
         dest.writeShort(opt.getId(version));
         opt.writeValue(obj, dest, version);
      }

      public int oneSerializedSize(Pair<DataType, Object> option, ProtocolVersion version) {
         DataType opt = (DataType)option.left;
         Object obj = option.right;
         return 2 + opt.serializedValueSize(obj, version);
      }
   }
}
