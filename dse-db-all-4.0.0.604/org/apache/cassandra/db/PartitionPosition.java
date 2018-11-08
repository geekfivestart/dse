package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IPartitionerDependentSerializer;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public abstract class PartitionPosition implements RingPosition<PartitionPosition> {
   protected final Token token;
   protected final PartitionPosition.Kind kind;
   public static final PartitionPosition.RowPositionSerializer serializer = new PartitionPosition.RowPositionSerializer();

   protected PartitionPosition(Token token, PartitionPosition.Kind kind) {
      this.token = token;
      this.kind = kind;
   }

   public final Token getToken() {
      return this.token;
   }

   public final PartitionPosition.Kind kind() {
      return this.kind;
   }

   public abstract boolean isMinimum();

   public abstract ByteSource asByteComparableSource();

   public static class RowPositionSerializer implements IPartitionerDependentSerializer<PartitionPosition, BoundsVersion> {
      public RowPositionSerializer() {
      }

      public void serialize(PartitionPosition pos, DataOutputPlus out, BoundsVersion version) throws IOException {
         PartitionPosition.Kind kind = pos.kind();
         out.writeByte(kind.ordinal());
         if(kind == PartitionPosition.Kind.ROW_KEY) {
            ByteBufferUtil.writeWithShortLength(((DecoratedKey)pos).getKey(), out);
         } else {
            Token.serializer.serialize(pos.getToken(), out, version);
         }

      }

      public PartitionPosition deserialize(DataInput in, IPartitioner p, BoundsVersion version) throws IOException {
         PartitionPosition.Kind kind = PartitionPosition.Kind.fromOrdinal(in.readByte());
         if(kind == PartitionPosition.Kind.ROW_KEY) {
            ByteBuffer k = ByteBufferUtil.readWithShortLength(in);
            return p.decorateKey(k);
         } else {
            Token t = Token.serializer.deserialize(in, p, version);
            return kind == PartitionPosition.Kind.MIN_BOUND?t.minKeyBound():t.maxKeyBound();
         }
      }

      public int serializedSize(PartitionPosition pos, BoundsVersion version) {
         PartitionPosition.Kind kind = pos.kind();
         int size = 1;
         int size;
         if(kind == PartitionPosition.Kind.ROW_KEY) {
            int keySize = ((DecoratedKey)pos).getKey().remaining();
            size = size + TypeSizes.sizeof((short)keySize) + keySize;
         } else {
            size = size + Token.serializer.serializedSize(pos.getToken(), version);
         }

         return size;
      }
   }

   public static final class ForKey {
      public ForKey() {
      }

      public static PartitionPosition get(ByteBuffer key, IPartitioner p) {
         return (PartitionPosition)(key != null && key.remaining() != 0?p.decorateKey(key):p.getMinimumToken().minKeyBound());
      }
   }

   public static enum Kind {
      ROW_KEY,
      MIN_BOUND,
      MAX_BOUND;

      private static final PartitionPosition.Kind[] allKinds = values();

      private Kind() {
      }

      static PartitionPosition.Kind fromOrdinal(int ordinal) {
         return allKinds[ordinal];
      }
   }
}
