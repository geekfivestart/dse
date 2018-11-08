package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicCompositeType extends AbstractCompositeType {
   private static final Logger logger = LoggerFactory.getLogger(DynamicCompositeType.class);
   private final Map<Byte, AbstractType<?>> aliases;
   private static final ConcurrentMap<Map<Byte, AbstractType<?>>, DynamicCompositeType> instances = new ConcurrentHashMap();

   public static synchronized DynamicCompositeType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      return getInstance(parser.getAliasParameters());
   }

   public static DynamicCompositeType getInstance(Map<Byte, AbstractType<?>> aliases) {
      DynamicCompositeType dct = (DynamicCompositeType)instances.get(aliases);
      if(dct == null) {
         dct = (DynamicCompositeType)instances.computeIfAbsent(aliases, (k) -> {
            return new DynamicCompositeType(k);
         });
      }

      return dct;
   }

   private DynamicCompositeType(Map<Byte, AbstractType<?>> aliases) {
      this.aliases = aliases;
   }

   protected boolean readIsStatic(ByteBuffer bb) {
      return false;
   }

   private AbstractType<?> getComparator(ByteBuffer bb) {
      try {
         int header = ByteBufferUtil.readShortLength(bb);
         if((header & '耀') == 0) {
            String name = ByteBufferUtil.string(ByteBufferUtil.readBytes(bb, header));
            return TypeParser.parse(name);
         } else {
            return (AbstractType)this.aliases.get(Byte.valueOf((byte)(header & 255)));
         }
      } catch (CharacterCodingException var4) {
         throw new RuntimeException(var4);
      }
   }

   protected AbstractType<?> getComparator(int i, ByteBuffer bb) {
      return this.getComparator(bb);
   }

   protected AbstractType<?> getComparator(int i, ByteBuffer bb1, ByteBuffer bb2) {
      AbstractType<?> comp1 = this.getComparator(bb1);
      AbstractType<?> comp2 = this.getComparator(bb2);
      AbstractType<?> rawComp = comp1;
      if(comp1 instanceof ReversedType && comp2 instanceof ReversedType) {
         comp1 = ((ReversedType)comp1).baseType;
         comp2 = ((ReversedType)comp2).baseType;
      }

      if(comp1 != comp2) {
         int cmp = comp1.getClass().getSimpleName().compareTo(comp2.getClass().getSimpleName());
         if(cmp != 0) {
            return cmp < 0?DynamicCompositeType.FixedValueComparator.alwaysLesserThan:DynamicCompositeType.FixedValueComparator.alwaysGreaterThan;
         }

         cmp = comp1.getClass().getName().compareTo(comp2.getClass().getName());
         if(cmp != 0) {
            return cmp < 0?DynamicCompositeType.FixedValueComparator.alwaysLesserThan:DynamicCompositeType.FixedValueComparator.alwaysGreaterThan;
         }
      }

      return rawComp;
   }

   public ByteSource asByteComparableSource(ByteBuffer byteBuffer) {
      List<ByteSource> srcs = new ArrayList();
      ByteBuffer bb = byteBuffer.duplicate();
      boolean isStatic = this.readIsStatic(bb);
      srcs.add(isStatic?null:ByteSource.empty());

      while(bb.remaining() > 0) {
         AbstractType<?> comp = this.getComparator(bb);
         srcs.add(ByteSource.of(comp.getClass().getSimpleName()));
         srcs.add(ByteSource.of(comp.getClass().getName()));
         srcs.add(comp.asByteComparableSource(ByteBufferUtil.readBytesWithShortLength(bb)));
         srcs.add(ByteSource.oneByte(bb.get()));
      }

      return ByteSource.of((ByteSource[])srcs.toArray(new ByteSource[0]));
   }

   protected AbstractType<?> getAndAppendComparator(int i, ByteBuffer bb, StringBuilder sb) {
      try {
         int header = ByteBufferUtil.readShortLength(bb);
         if((header & '耀') == 0) {
            String name = ByteBufferUtil.string(ByteBufferUtil.readBytes(bb, header));
            sb.append(name).append("@");
            return TypeParser.parse(name);
         } else {
            sb.append((char)(header & 255)).append("@");
            return (AbstractType)this.aliases.get(Byte.valueOf((byte)(header & 255)));
         }
      } catch (CharacterCodingException var6) {
         throw new RuntimeException(var6);
      }
   }

   protected AbstractCompositeType.ParsedComparator parseComparator(int i, String part) {
      return new DynamicCompositeType.DynamicParsedComparator(part);
   }

   protected AbstractType<?> validateComparator(int i, ByteBuffer bb) throws MarshalException {
      AbstractType<?> comparator = null;
      if(bb.remaining() < 2) {
         throw new MarshalException("Not enough bytes to header of the comparator part of component " + i);
      } else {
         int header = ByteBufferUtil.readShortLength(bb);
         if((header & '耀') == 0) {
            if(bb.remaining() < header) {
               throw new MarshalException("Not enough bytes to read comparator name of component " + i);
            }

            ByteBuffer value = ByteBufferUtil.readBytes(bb, header);
            String valueStr = null;

            try {
               valueStr = ByteBufferUtil.string(value);
               comparator = TypeParser.parse(valueStr);
            } catch (CharacterCodingException var8) {
               logger.error("Failed when decoding the byte buffer in ByteBufferUtil.string()", var8);
            } catch (Exception var9) {
               logger.error("Failed to parse value string \"{}\" with exception:", valueStr, var9);
            }
         } else {
            comparator = (AbstractType)this.aliases.get(Byte.valueOf((byte)(header & 255)));
         }

         if(comparator == null) {
            throw new MarshalException("Cannot find comparator for component " + i);
         } else {
            return comparator;
         }
      }
   }

   public ByteBuffer decompose(Object... objects) {
      throw new UnsupportedOperationException();
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      if(this == previous) {
         return true;
      } else if(!(previous instanceof DynamicCompositeType)) {
         return false;
      } else {
         DynamicCompositeType cp = (DynamicCompositeType)previous;
         if(this.aliases.size() < cp.aliases.size()) {
            return false;
         } else {
            Iterator var3 = cp.aliases.entrySet().iterator();

            AbstractType tprev;
            AbstractType tnew;
            do {
               if(!var3.hasNext()) {
                  return true;
               }

               Entry<Byte, AbstractType<?>> entry = (Entry)var3.next();
               tprev = (AbstractType)entry.getValue();
               tnew = (AbstractType)this.aliases.get(entry.getKey());
            } while(tnew != null && tnew == tprev);

            return false;
         }
      }
   }

   public String toString() {
      return this.getClass().getName() + TypeParser.stringifyAliasesParameters(this.aliases);
   }

   private static class FixedValueComparator extends AbstractType<Void> {
      public static final DynamicCompositeType.FixedValueComparator alwaysLesserThan = new DynamicCompositeType.FixedValueComparator(-1);
      public static final DynamicCompositeType.FixedValueComparator alwaysGreaterThan = new DynamicCompositeType.FixedValueComparator(1);

      public FixedValueComparator(int cmp) {
         super(AbstractType.ComparisonType.FIXED_COMPARE, -1, AbstractType.PrimitiveType.NONE, cmp);
      }

      public Void compose(ByteBuffer bytes) {
         throw new UnsupportedOperationException();
      }

      public ByteBuffer decompose(Void value) {
         throw new UnsupportedOperationException();
      }

      public String getString(ByteBuffer bytes) {
         throw new UnsupportedOperationException();
      }

      public ByteBuffer fromString(String str) {
         throw new UnsupportedOperationException();
      }

      public Term fromJSONObject(Object parsed) {
         throw new UnsupportedOperationException();
      }

      public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
         throw new UnsupportedOperationException();
      }

      public void validate(ByteBuffer bytes) {
         throw new UnsupportedOperationException();
      }

      public TypeSerializer<Void> getSerializer() {
         throw new UnsupportedOperationException();
      }
   }

   private class DynamicParsedComparator implements AbstractCompositeType.ParsedComparator {
      final AbstractType<?> type;
      final boolean isAlias;
      final String comparatorName;
      final String remainingPart;

      DynamicParsedComparator(String part) {
         String[] splits = part.split("@");
         if(splits.length != 2) {
            throw new IllegalArgumentException("Invalid component representation: " + part);
         } else {
            this.comparatorName = splits[0];
            this.remainingPart = splits[1];

            try {
               AbstractType<?> t = null;
               if(this.comparatorName.length() == 1) {
                  t = (AbstractType)DynamicCompositeType.this.aliases.get(Byte.valueOf((byte)this.comparatorName.charAt(0)));
               }

               this.isAlias = t != null;
               if(!this.isAlias) {
                  t = TypeParser.parse(this.comparatorName);
               }

               this.type = t;
            } catch (ConfigurationException | SyntaxException var5) {
               throw new IllegalArgumentException(var5);
            }
         }
      }

      public AbstractType<?> getAbstractType() {
         return this.type;
      }

      public String getRemainingPart() {
         return this.remainingPart;
      }

      public int getComparatorSerializedSize() {
         return this.isAlias?2:2 + ByteBufferUtil.bytes(this.comparatorName).remaining();
      }

      public void serializeComparator(ByteBuffer bb) {
         int headerx = false;
         int header;
         if(this.isAlias) {
            header = '耀' | (byte)this.comparatorName.charAt(0) & 255;
         } else {
            header = this.comparatorName.length();
         }

         ByteBufferUtil.writeShortLength(bb, header);
         if(!this.isAlias) {
            bb.put(ByteBufferUtil.bytes(this.comparatorName));
         }

      }
   }
}
