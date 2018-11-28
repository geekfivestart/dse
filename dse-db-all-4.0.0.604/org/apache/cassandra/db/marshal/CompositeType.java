package org.apache.cassandra.db.marshal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public class CompositeType extends AbstractCompositeType {
   private static final int STATIC_MARKER = 65535;
   public final List<AbstractType<?>> types;
   private static final ConcurrentMap<List<AbstractType<?>>, CompositeType> instances = new ConcurrentHashMap();

   public static CompositeType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      return getInstance(parser.getTypeParameters());
   }

   public static CompositeType getInstance(Iterable<AbstractType<?>> types) {
      return getInstance((List)Lists.newArrayList(types));
   }

   public static CompositeType getInstance(AbstractType... types) {
      return getInstance(Arrays.asList(types));
   }

   protected boolean readIsStatic(ByteBuffer bb) {
      return readStatic(bb);
   }

   private static boolean readStatic(ByteBuffer bb) {
      if(bb.remaining() < 2) {
         return false;
      } else {
         int header = ByteBufferUtil.getShortLength(bb, bb.position());
         if((header & '\uffff') != '\uffff') {
            return false;
         } else {
            ByteBufferUtil.readShortLength(bb);
            return true;
         }
      }
   }

   public static CompositeType getInstance(List<AbstractType<?>> types) {
      assert types != null && !types.isEmpty();

      CompositeType ct = (CompositeType)instances.get(types);
      if(ct == null) {
         ct = new CompositeType(types);
         CompositeType previous = (CompositeType)instances.putIfAbsent(types, ct);
         if(previous != null) {
            ct = previous;
         }
      }

      return ct;
   }

   protected CompositeType(List<AbstractType<?>> types) {
      this.types = ImmutableList.copyOf(types);
   }

   protected AbstractType<?> getComparator(int i, ByteBuffer bb) {
      try {
         return (AbstractType)this.types.get(i);
      } catch (IndexOutOfBoundsException var4) {
         throw new RuntimeException("Cannot get comparator " + i + " in " + this + ". This might due to a mismatch between the schema and the data read", var4);
      }
   }

   protected AbstractType<?> getComparator(int i, ByteBuffer bb1, ByteBuffer bb2) {
      return this.getComparator(i, bb1);
   }

   protected AbstractType<?> getAndAppendComparator(int i, ByteBuffer bb, StringBuilder sb) {
      return (AbstractType)this.types.get(i);
   }

   public ByteSource asByteComparableSource(ByteBuffer byteBuffer) {
      if(byteBuffer != null && byteBuffer.remaining() != 0) {
         ByteSource[] srcs = new ByteSource[this.types.size() * 2 + 1];
         ByteBuffer bb = byteBuffer.duplicate();
         boolean isStatic = readStatic(bb);
         srcs[0] = isStatic?null:ByteSource.empty();

         int i;
         for(i = 0; bb.remaining() > 0; ++i) {
            srcs[i * 2 + 1] = ((AbstractType)this.types.get(i)).asByteComparableSource(ByteBufferUtil.readBytesWithShortLength(bb));
            srcs[i * 2 + 2] = ByteSource.oneByte(bb.get() & 255 ^ 128);
         }

         if(i * 2 + 1 < srcs.length) {
            srcs = (ByteSource[])Arrays.copyOfRange(srcs, 0, i * 2 + 1);
         }

         return ByteSource.of(srcs);
      } else {
         return null;
      }
   }

   protected AbstractCompositeType.ParsedComparator parseComparator(int i, String part) {
      return new CompositeType.StaticParsedComparator((AbstractType)this.types.get(i), part);
   }

   protected AbstractType<?> validateComparator(int i, ByteBuffer bb) throws MarshalException {
      if(i >= this.types.size()) {
         throw new MarshalException("Too many bytes for comparator");
      } else {
         return (AbstractType)this.types.get(i);
      }
   }

   public ByteBuffer decompose(Object... objects) {
      assert objects.length == this.types.size();

      ByteBuffer[] serialized = new ByteBuffer[objects.length];

      for(int i = 0; i < objects.length; ++i) {
         ByteBuffer buffer = ((AbstractType)this.types.get(i)).decompose(objects[i]);
         serialized[i] = buffer;
      }

      return build(serialized);
   }

   public ByteBuffer[] split(ByteBuffer name) {
      ByteBuffer[] l = new ByteBuffer[this.types.size()];
      ByteBuffer bb = name.duplicate();
      readStatic(bb);
      int i = 0;

      while(bb.remaining() > 0) {
         l[i++] = ByteBufferUtil.readBytesWithShortLength(bb);
         bb.get();
      }

      return i == l.length?l:(ByteBuffer[])Arrays.copyOfRange(l, 0, i);
   }

   public static List<ByteBuffer> splitName(ByteBuffer name) {
      List<ByteBuffer> l = new ArrayList();
      ByteBuffer bb = name.duplicate();
      readStatic(bb);

      while(bb.remaining() > 0) {
         l.add(ByteBufferUtil.readBytesWithShortLength(bb));
         bb.get();
      }

      return l;
   }

   public static ByteBuffer extractComponent(ByteBuffer bb, int idx) {
      bb = bb.duplicate();
      readStatic(bb);

      for(int i = 0; bb.remaining() > 0; ++i) {
         ByteBuffer c = ByteBufferUtil.readBytesWithShortLength(bb);
         if(i == idx) {
            return c;
         }

         bb.get();
      }

      return null;
   }

   public static boolean isStaticName(ByteBuffer bb) {
      return bb.remaining() >= 2 && (ByteBufferUtil.getShortLength(bb, bb.position()) & '\uffff') == '\uffff';
   }

   public int componentsCount() {
      return this.types.size();
   }

   public List<AbstractType<?>> getComponents() {
      return this.types;
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      if(this == previous) {
         return true;
      } else if(!(previous instanceof CompositeType)) {
         return false;
      } else {
         CompositeType cp = (CompositeType)previous;
         if(this.types.size() < cp.types.size()) {
            return false;
         } else {
            for(int i = 0; i < cp.types.size(); ++i) {
               AbstractType tprev = (AbstractType)cp.types.get(i);
               AbstractType tnew = (AbstractType)this.types.get(i);
               if(!tnew.isCompatibleWith(tprev)) {
                  return false;
               }
            }

            return true;
         }
      }
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      if(this == otherType) {
         return true;
      } else if(!(otherType instanceof CompositeType)) {
         return false;
      } else {
         CompositeType cp = (CompositeType)otherType;
         if(this.types.size() < cp.types.size()) {
            return false;
         } else {
            for(int i = 0; i < cp.types.size(); ++i) {
               AbstractType tprev = (AbstractType)cp.types.get(i);
               AbstractType tnew = (AbstractType)this.types.get(i);
               if(!tnew.isValueCompatibleWith(tprev)) {
                  return false;
               }
            }

            return true;
         }
      }
   }

   public String toString() {
      return this.getClass().getName() + TypeParser.stringifyTypeParameters(this.types);
   }

   public static ByteBuffer build(ByteBuffer... buffers) {
      return build(false, buffers);
   }

   public static /* varargs */ ByteBuffer build(boolean isStatic, ByteBuffer ... buffers) {
      int totalLength = isStatic ? 2 : 0;
      for (ByteBuffer bb : buffers) {
         totalLength += 2 + bb.remaining() + 1;
      }
      ByteBuffer out = ByteBuffer.allocate(totalLength);
      if (isStatic) {
         out.putShort((short)-1);
      }
      for (ByteBuffer bb : buffers) {
         ByteBufferUtil.writeShortLength(out, bb.remaining());
         int toCopy = bb.remaining();
         ByteBufferUtil.arrayCopy(bb, bb.position(), out, out.position(), toCopy);
         out.position(out.position() + toCopy);
         out.put((byte)0);
      }
      out.flip();
      return out;
   }

   private static class StaticParsedComparator implements AbstractCompositeType.ParsedComparator {
      final AbstractType<?> type;
      final String part;

      StaticParsedComparator(AbstractType<?> type, String part) {
         this.type = type;
         this.part = part;
      }

      public AbstractType<?> getAbstractType() {
         return this.type;
      }

      public String getRemainingPart() {
         return this.part;
      }

      public int getComparatorSerializedSize() {
         return 0;
      }

      public void serializeComparator(ByteBuffer bb) {
      }
   }
}
