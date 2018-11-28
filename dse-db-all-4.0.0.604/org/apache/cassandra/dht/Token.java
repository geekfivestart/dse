package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public abstract class Token implements RingPosition<Token>, Serializable {
   private static final long serialVersionUID = 1L;
   public static final Token.TokenSerializer serializer = new Token.TokenSerializer();

   public Token() {
   }

   public abstract IPartitioner getPartitioner();

   public abstract long getHeapSize();

   public abstract Object getTokenValue();

   public abstract ByteSource asByteComparableSource();

   public abstract double size(Token var1);

   public abstract Token increaseSlightly();

   public Token getToken() {
      return this;
   }

   public Token minValue() {
      return this.getPartitioner().getMinimumToken();
   }

   public boolean isMinimum() {
      return this.equals(this.minValue());
   }

   public Token.KeyBound minKeyBound() {
      return new Token.KeyBound(this, true);
   }

   public Token.KeyBound maxKeyBound() {
      return this.isMinimum()?this.minKeyBound():new Token.KeyBound(this, false);
   }

   public <R extends RingPosition<R>> R upperBound(Class<R> klass) {
      return (R)(klass.equals(this.getClass())?this:this.maxKeyBound());
   }

   public static class KeyBound extends PartitionPosition {
      private KeyBound(Token t, boolean isMinimumBound) {
         super(t, isMinimumBound?PartitionPosition.Kind.MIN_BOUND:PartitionPosition.Kind.MAX_BOUND);
      }

      public int compareTo(PartitionPosition pos) {
         if(this == pos) {
            return 0;
         } else {
            int cmp = this.token.compareTo(pos.getToken());
            return cmp != 0?cmp:(this.kind == PartitionPosition.Kind.MIN_BOUND?(pos.kind() == PartitionPosition.Kind.MIN_BOUND?0:-1):(pos.kind() == PartitionPosition.Kind.MAX_BOUND?0:1));
         }
      }

      public ByteSource asByteComparableSource() {
         return ByteSource.withTerminator(this.kind == PartitionPosition.Kind.MIN_BOUND?32:96, new ByteSource[]{this.token.asByteComparableSource()});
      }

      public IPartitioner getPartitioner() {
         return this.token.getPartitioner();
      }

      public Token.KeyBound minValue() {
         return this.getPartitioner().getMinimumToken().minKeyBound();
      }

      public boolean isMinimum() {
         return this.token.isMinimum();
      }

      public boolean equals(Object obj) {
         if(this == obj) {
            return true;
         } else if(obj != null && this.getClass() == obj.getClass()) {
            Token.KeyBound other = (Token.KeyBound)obj;
            return this.token.equals(other.token) && this.kind == other.kind;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return this.token.hashCode() + (this.kind == PartitionPosition.Kind.MIN_BOUND?0:1);
      }

      public String toString() {
         return String.format("%s(%s)", new Object[]{this.kind == PartitionPosition.Kind.MIN_BOUND?"min":"max", this.token.toString()});
      }
   }

   public static class TokenSerializer implements IPartitionerDependentSerializer<Token, BoundsVersion> {
      public TokenSerializer() {
      }

      public void serialize(Token token, DataOutputPlus out, BoundsVersion version) throws IOException {
         IPartitioner p = token.getPartitioner();
         ByteBuffer b = p.getTokenFactory().toByteArray(token);
         ByteBufferUtil.writeWithLength(b, out);
      }

      public Token deserialize(DataInput in, IPartitioner p, BoundsVersion version) throws IOException {
         int size = in.readInt();
         byte[] bytes = new byte[size];
         in.readFully(bytes);
         return p.getTokenFactory().fromByteArray(ByteBuffer.wrap(bytes));
      }

      public int serializedSize(Token object, BoundsVersion version) {
         IPartitioner p = object.getPartitioner();
         ByteBuffer b = p.getTokenFactory().toByteArray(object);
         return TypeSizes.sizeof(b.remaining()) + b.remaining();
      }
   }

   public abstract static class TokenFactory {
      public TokenFactory() {
      }

      public abstract ByteBuffer toByteArray(Token var1);

      public abstract Token fromByteArray(ByteBuffer var1);

      public abstract String toString(Token var1);

      public abstract Token fromString(String var1);

      public abstract void validate(String var1) throws ConfigurationException;
   }
}
