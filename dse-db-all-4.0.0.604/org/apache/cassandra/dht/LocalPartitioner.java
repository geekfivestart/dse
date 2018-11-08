package org.apache.cassandra.dht;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.HeapAllocator;

public class LocalPartitioner implements IPartitioner {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new LocalPartitioner((AbstractType)null).new LocalToken(null));
   final AbstractType<?> comparator;
   private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
      public ByteBuffer toByteArray(Token token) {
         return (ByteBuffer)((LocalPartitioner.LocalToken)token).token;
      }

      public Token fromByteArray(ByteBuffer bytes) {
         return LocalPartitioner.this.new LocalToken(bytes);
      }

      public String toString(Token token) {
         return LocalPartitioner.this.comparator.getString((ByteBuffer)((LocalPartitioner.LocalToken)token).token);
      }

      public void validate(String token) {
         LocalPartitioner.this.comparator.validate(LocalPartitioner.this.comparator.fromString(token));
      }

      public Token fromString(String string) {
         return LocalPartitioner.this.new LocalToken(LocalPartitioner.this.comparator.fromString(string));
      }
   };

   public LocalPartitioner(AbstractType<?> comparator) {
      this.comparator = comparator;
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      return new CachedHashDecoratedKey(this.getToken(key), key);
   }

   public Token midpoint(Token left, Token right) {
      throw new UnsupportedOperationException();
   }

   public Token split(Token left, Token right, double ratioToLeft) {
      throw new UnsupportedOperationException();
   }

   public LocalPartitioner.LocalToken getMinimumToken() {
      return new LocalPartitioner.LocalToken(ByteBufferUtil.EMPTY_BYTE_BUFFER);
   }

   public LocalPartitioner.LocalToken getToken(ByteBuffer key) {
      return new LocalPartitioner.LocalToken(key);
   }

   public LocalPartitioner.LocalToken getRandomToken() {
      throw new UnsupportedOperationException();
   }

   public LocalPartitioner.LocalToken getRandomToken(Random random) {
      throw new UnsupportedOperationException();
   }

   public Token.TokenFactory getTokenFactory() {
      return this.tokenFactory;
   }

   public boolean preservesOrder() {
      return true;
   }

   public Map<Token, Float> describeOwnership(List<Token> sortedTokens) {
      return Collections.singletonMap(this.getMinimumToken(), new Float(1.0D));
   }

   public AbstractType<?> getTokenValidator() {
      return this.comparator;
   }

   public AbstractType<?> partitionOrdering() {
      return this.comparator;
   }

   public class LocalToken extends ComparableObjectToken<ByteBuffer> {
      static final long serialVersionUID = 8437543776403014875L;
      private int hashCode;

      private LocalToken() {
         super((Comparable)null);
         this.hashCode = -1;
      }

      public LocalToken(ByteBuffer token) {
         super(HeapAllocator.instance.clone(token));
         this.hashCode = -1;
      }

      public String toString() {
         return LocalPartitioner.this.comparator.getString((ByteBuffer)this.token);
      }

      public int compareTo(Token o) {
         assert this.getPartitioner() == o.getPartitioner();

         return LocalPartitioner.this.comparator.compare((ByteBuffer)this.token, (ByteBuffer)((LocalPartitioner.LocalToken)o).token);
      }

      public ByteSource asByteComparableSource() {
         return LocalPartitioner.this.comparator.asByteComparableSource((ByteBuffer)this.token);
      }

      public int hashCode() {
         int currHashCode = this.hashCode;
         if(currHashCode == -1) {
            currHashCode = 31 + ((ByteBuffer)this.token).hashCode();
            this.hashCode = currHashCode;
         }

         return currHashCode;
      }

      public boolean equals(Object obj) {
         if(this == obj) {
            return true;
         } else if(!(obj instanceof LocalPartitioner.LocalToken)) {
            return false;
         } else {
            LocalPartitioner.LocalToken other = (LocalPartitioner.LocalToken)obj;
            return ((ByteBuffer)this.token).equals(other.token);
         }
      }

      public IPartitioner getPartitioner() {
         return LocalPartitioner.this;
      }

      public long getHeapSize() {
         return LocalPartitioner.EMPTY_SIZE + ObjectSizes.sizeOnHeapOf((ByteBuffer)this.token);
      }
   }
}
