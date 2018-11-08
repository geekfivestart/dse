package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractCompositeType extends AbstractType<ByteBuffer> {
   private static final String COLON = ":";
   private static final Pattern COLON_PAT = Pattern.compile(":");
   private static final String ESCAPED_COLON = "\\\\:";
   private static final Pattern ESCAPED_COLON_PAT = Pattern.compile("\\\\:");

   protected AbstractCompositeType() {
      super(AbstractType.ComparisonType.CUSTOM, -1);
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      ByteBuffer bb1 = o1.duplicate();
      ByteBuffer bb2 = o2.duplicate();
      boolean isStatic1 = this.readIsStatic(bb1);
      boolean isStatic2 = this.readIsStatic(bb2);
      if(isStatic1 != isStatic2) {
         return isStatic1?-1:1;
      } else {
         int i = 0;

         for(ByteBuffer previous = null; bb1.remaining() > 0 && bb2.remaining() > 0; ++i) {
            AbstractType<?> comparator = this.getComparator(i, bb1, bb2);
            ByteBuffer value1 = ByteBufferUtil.readBytesWithShortLength(bb1);
            ByteBuffer value2 = ByteBufferUtil.readBytesWithShortLength(bb2);
            int cmp = comparator.compareCollectionMembers(value1, value2, previous);
            if(cmp != 0) {
               return cmp;
            }

            previous = value1;
            byte b1 = bb1.get();
            byte b2 = bb2.get();
            if(b1 != b2) {
               return b1 - b2;
            }
         }

         return bb1.remaining() == 0?(bb2.remaining() == 0?0:-1):1;
      }
   }

   protected abstract boolean readIsStatic(ByteBuffer var1);

   public ByteBuffer[] split(ByteBuffer name) {
      List<ByteBuffer> l = new ArrayList();
      ByteBuffer bb = name.duplicate();
      this.readIsStatic(bb);
      int var4 = 0;

      while(bb.remaining() > 0) {
         this.getComparator(var4++, bb);
         l.add(ByteBufferUtil.readBytesWithShortLength(bb));
         bb.get();
      }

      return (ByteBuffer[])l.toArray(ByteBufferUtil.EMPTY_BUFFER_ARRAY);
   }

   public static String escape(String input) {
      if(input.isEmpty()) {
         return input;
      } else {
         String res = COLON_PAT.matcher(input).replaceAll("\\\\:");
         char last = res.charAt(res.length() - 1);
         return last != 92 && last != 33?res:res + '!';
      }
   }

   static String unescape(String input) {
      if(input.isEmpty()) {
         return input;
      } else {
         String res = ESCAPED_COLON_PAT.matcher(input).replaceAll(":");
         char last = res.charAt(res.length() - 1);
         return last == 33?res.substring(0, res.length() - 1):res;
      }
   }

   static List<String> split(String input) {
      if(input.isEmpty()) {
         return Collections.emptyList();
      } else {
         List<String> res = new ArrayList();
         int prev = 0;

         for(int i = 0; i < input.length(); ++i) {
            if(input.charAt(i) == 58 && (i <= 0 || input.charAt(i - 1) != 92)) {
               res.add(input.substring(prev, i));
               prev = i + 1;
            }
         }

         res.add(input.substring(prev, input.length()));
         return res;
      }
   }

   public String getString(ByteBuffer bytes) {
      StringBuilder sb = new StringBuilder();
      ByteBuffer bb = bytes.duplicate();
      this.readIsStatic(bb);

      for(int i = 0; bb.remaining() > 0; ++i) {
         if(bb.remaining() != bytes.remaining()) {
            sb.append(":");
         }

         AbstractType<?> comparator = this.getAndAppendComparator(i, bb, sb);
         ByteBuffer value = ByteBufferUtil.readBytesWithShortLength(bb);
         sb.append(escape(comparator.getString(value)));
         byte b = bb.get();
         if(b != 0) {
            sb.append(b < 0?":_":":!");
            break;
         }
      }

      return sb.toString();
   }

   public ByteBuffer fromString(String source) {
      List<String> parts = split(source);
      List<ByteBuffer> components = new ArrayList(parts.size());
      List<AbstractCompositeType.ParsedComparator> comparators = new ArrayList(parts.size());
      int totalLength = 0;
      int i = 0;
      boolean lastByteIsOne = false;
      boolean lastByteIsMinusOne = false;

      for(Iterator var9 = parts.iterator(); var9.hasNext(); ++i) {
         String part = (String)var9.next();
         if(part.equals("!")) {
            lastByteIsOne = true;
            break;
         }

         if(part.equals("_")) {
            lastByteIsMinusOne = true;
            break;
         }

         AbstractCompositeType.ParsedComparator p = this.parseComparator(i, part);
         AbstractType<?> type = p.getAbstractType();
         part = p.getRemainingPart();
         ByteBuffer component = type.fromString(unescape(part));
         totalLength += p.getComparatorSerializedSize() + 2 + component.remaining() + 1;
         components.add(component);
         comparators.add(p);
      }

      ByteBuffer bb = ByteBuffer.allocate(totalLength);
      i = 0;

      for(Iterator var15 = components.iterator(); var15.hasNext(); ++i) {
         ByteBuffer component = (ByteBuffer)var15.next();
         ((AbstractCompositeType.ParsedComparator)comparators.get(i)).serializeComparator(bb);
         ByteBufferUtil.writeShortLength(bb, component.remaining());
         bb.put(component);
         bb.put(0);
      }

      if(lastByteIsOne) {
         bb.put(bb.limit() - 1, 1);
      } else if(lastByteIsMinusOne) {
         bb.put(bb.limit() - 1, -1);
      }

      bb.rewind();
      return bb;
   }

   public Term fromJSONObject(Object parsed) {
      throw new UnsupportedOperationException();
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      ByteBuffer bb = bytes.duplicate();
      this.readIsStatic(bb);
      int i = 0;

      for(ByteBuffer previous = null; bb.remaining() > 0; ++i) {
         AbstractType<?> comparator = this.validateComparator(i, bb);
         if(bb.remaining() < 2) {
            throw new MarshalException("Not enough bytes to read value size of component " + i);
         }

         int length = ByteBufferUtil.readShortLength(bb);
         if(bb.remaining() < length) {
            throw new MarshalException("Not enough bytes to read value of component " + i);
         }

         ByteBuffer value = ByteBufferUtil.readBytes(bb, length);
         comparator.validateCollectionMember(value, previous);
         if(bb.remaining() == 0) {
            throw new MarshalException("Not enough bytes to read the end-of-component byte of component" + i);
         }

         byte b = bb.get();
         if(b != 0 && bb.remaining() != 0) {
            throw new MarshalException("Invalid bytes remaining after an end-of-component at component" + i);
         }

         previous = value;
      }

   }

   public abstract ByteBuffer decompose(Object... var1);

   public TypeSerializer<ByteBuffer> getSerializer() {
      return BytesSerializer.instance;
   }

   public boolean referencesUserType(String name) {
      return this.getComponents().stream().anyMatch((f) -> {
         return f.referencesUserType(name);
      });
   }

   protected abstract AbstractType<?> getComparator(int var1, ByteBuffer var2);

   protected abstract AbstractType<?> getComparator(int var1, ByteBuffer var2, ByteBuffer var3);

   protected abstract AbstractType<?> getAndAppendComparator(int var1, ByteBuffer var2, StringBuilder var3);

   protected abstract AbstractType<?> validateComparator(int var1, ByteBuffer var2) throws MarshalException;

   protected abstract AbstractCompositeType.ParsedComparator parseComparator(int var1, String var2);

   protected interface ParsedComparator {
      AbstractType<?> getAbstractType();

      String getRemainingPart();

      int getComparatorSerializedSize();

      void serializeComparator(ByteBuffer var1);
   }
}
