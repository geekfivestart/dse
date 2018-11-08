package org.apache.cassandra.dht;

import com.google.common.primitives.Longs;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PreHashedDecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.ObjectSizes;

public class Murmur3Partitioner implements IPartitioner {
   public static final Murmur3Partitioner.LongToken MINIMUM = new Murmur3Partitioner.LongToken(-9223372036854775808L);
   public static final long MAXIMUM = 9223372036854775807L;
   private static final int HEAP_SIZE;
   public static final Murmur3Partitioner instance;
   public static final AbstractType<?> partitionOrdering;
   private final Splitter splitter = new Splitter(this);
   private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
      public ByteBuffer toByteArray(Token token) {
         Murmur3Partitioner.LongToken longToken = (Murmur3Partitioner.LongToken)token;
         return ByteBufferUtil.bytes(longToken.token);
      }

      public Token fromByteArray(ByteBuffer bytes) {
         return new Murmur3Partitioner.LongToken(ByteBufferUtil.toLong(bytes));
      }

      public String toString(Token token) {
         return token.toString();
      }

      public void validate(String token) throws ConfigurationException {
         try {
            this.fromString(token);
         } catch (NumberFormatException var3) {
            throw new ConfigurationException(var3.getMessage());
         }
      }

      public Token fromString(String string) {
         try {
            return new Murmur3Partitioner.LongToken(Long.parseLong(string));
         } catch (NumberFormatException var3) {
            throw new IllegalArgumentException(String.format("Invalid token for Murmur3Partitioner. Got %s but expected a long value (unsigned 8 bytes integer).", new Object[]{string}));
         }
      }
   };

   public Murmur3Partitioner() {
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      long[] hash = this.getHash(key);
      return new PreHashedDecoratedKey(this.getToken(key, hash), key, hash[0], hash[1]);
   }

   public Token midpoint(Token lToken, Token rToken) {
      BigInteger l = BigInteger.valueOf(((Murmur3Partitioner.LongToken)lToken).token);
      BigInteger r = BigInteger.valueOf(((Murmur3Partitioner.LongToken)rToken).token);
      BigInteger midpoint;
      BigInteger max;
      if(l.compareTo(r) < 0) {
         max = l.add(r);
         midpoint = max.shiftRight(1);
      } else {
         max = BigInteger.valueOf(9223372036854775807L);
         BigInteger min = BigInteger.valueOf(MINIMUM.token);
         midpoint = max.subtract(min).add(l).add(r).shiftRight(1);
         if(midpoint.compareTo(max) > 0) {
            midpoint = min.add(midpoint.subtract(max));
         }
      }

      return new Murmur3Partitioner.LongToken(midpoint.longValue());
   }

   public Token split(Token lToken, Token rToken, double ratioToLeft) {
      assert ratioToLeft >= 0.0D && ratioToLeft <= 1.0D;

      BigDecimal l = BigDecimal.valueOf(((Murmur3Partitioner.LongToken)lToken).token);
      BigDecimal r = BigDecimal.valueOf(((Murmur3Partitioner.LongToken)rToken).token);
      BigDecimal ratio = BigDecimal.valueOf(ratioToLeft);
      long newToken;
      if(l.compareTo(r) < 0) {
         newToken = r.subtract(l).multiply(ratio).add(l).toBigInteger().longValue();
      } else {
         BigDecimal max = BigDecimal.valueOf(9223372036854775807L);
         BigDecimal min = BigDecimal.valueOf(MINIMUM.token);
         BigInteger token = max.subtract(min).add(r).subtract(l).multiply(ratio).add(l).toBigInteger();
         BigInteger maxToken = BigInteger.valueOf(9223372036854775807L);
         if(token.compareTo(maxToken) <= 0) {
            newToken = token.longValue();
         } else {
            BigInteger minToken = BigInteger.valueOf(MINIMUM.token);
            newToken = minToken.add(token.subtract(maxToken)).longValue();
         }
      }

      return new Murmur3Partitioner.LongToken(newToken);
   }

   public Murmur3Partitioner.LongToken getMinimumToken() {
      return MINIMUM;
   }

   public Murmur3Partitioner.LongToken getToken(ByteBuffer key) {
      return this.getToken(key, this.getHash(key));
   }

   private Murmur3Partitioner.LongToken getToken(ByteBuffer key, long[] hash) {
      return key.remaining() == 0?MINIMUM:new Murmur3Partitioner.LongToken(this.normalize(hash[0]));
   }

   private long[] getHash(ByteBuffer key) {
      long[] hash = new long[2];
      MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0L, hash);
      return hash;
   }

   public Murmur3Partitioner.LongToken getRandomToken() {
      return this.getRandomToken(ThreadLocalRandom.current());
   }

   public Murmur3Partitioner.LongToken getRandomToken(Random r) {
      return new Murmur3Partitioner.LongToken(this.normalize(r.nextLong()));
   }

   private long normalize(long v) {
      return v == -9223372036854775808L?9223372036854775807L:v;
   }

   public boolean preservesOrder() {
      return false;
   }

   public Map<Token, Float> describeOwnership(List<Token> sortedTokens) {
      Map<Token, Float> ownerships = new HashMap();
      Iterator<Token> i = sortedTokens.iterator();
      if(!i.hasNext()) {
         throw new RuntimeException("No nodes present in the cluster. Has this node finished starting up?");
      } else {
         if(sortedTokens.size() == 1) {
            ownerships.put(i.next(), new Float(1.0D));
         } else {
            BigInteger ri = BigInteger.valueOf(9223372036854775807L).subtract(BigInteger.valueOf(MINIMUM.token + 1L));
            BigDecimal r = new BigDecimal(ri);
            Token start = (Token)i.next();
            BigInteger ti = BigInteger.valueOf(((Murmur3Partitioner.LongToken)start).token);

            float x;
            for(BigInteger tim1 = ti; i.hasNext(); tim1 = ti) {
               Token t = (Token)i.next();
               ti = BigInteger.valueOf(((Murmur3Partitioner.LongToken)t).token);
               x = (new BigDecimal(ti.subtract(tim1).add(ri).mod(ri))).divide(r, 6, 6).floatValue();
               ownerships.put(t, Float.valueOf(x));
            }

            x = (new BigDecimal(BigInteger.valueOf(((Murmur3Partitioner.LongToken)start).token).subtract(ti).add(ri).mod(ri))).divide(r, 6, 6).floatValue();
            ownerships.put(start, Float.valueOf(x));
         }

         return ownerships;
      }
   }

   public Token.TokenFactory getTokenFactory() {
      return this.tokenFactory;
   }

   public AbstractType<?> getTokenValidator() {
      return LongType.instance;
   }

   public Token getMaximumToken() {
      return new Murmur3Partitioner.LongToken(9223372036854775807L);
   }

   public AbstractType<?> partitionOrdering() {
      return partitionOrdering;
   }

   public Optional<Splitter> splitter() {
      return Optional.of(this.splitter);
   }

   public boolean hasNumericTokens() {
      return true;
   }

   public BigInteger valueForToken(Token token) {
      return BigInteger.valueOf(((Murmur3Partitioner.LongToken)token).token);
   }

   static {
      HEAP_SIZE = (int)ObjectSizes.measureDeep(MINIMUM);
      instance = new Murmur3Partitioner();
      partitionOrdering = new PartitionerDefinedOrder(instance);
   }

   public static class LongToken extends Token {
      static final long serialVersionUID = -5833580143318243006L;
      final long token;

      public LongToken(long token) {
         this.token = token;
      }

      public String toString() {
         return Long.toString(this.token);
      }

      public boolean equals(Object obj) {
         return this == obj?true:(obj != null && this.getClass() == obj.getClass()?this.token == ((Murmur3Partitioner.LongToken)obj).token:false);
      }

      public int hashCode() {
         return Longs.hashCode(this.token);
      }

      public int compareTo(Token o) {
         return Long.compare(this.token, ((Murmur3Partitioner.LongToken)o).token);
      }

      public ByteSource asByteComparableSource() {
         return ByteSource.of(this.token);
      }

      public IPartitioner getPartitioner() {
         return Murmur3Partitioner.instance;
      }

      public long getHeapSize() {
         return (long)Murmur3Partitioner.HEAP_SIZE;
      }

      public Object getTokenValue() {
         return Long.valueOf(this.token);
      }

      public double size(Token next) {
         Murmur3Partitioner.LongToken n = (Murmur3Partitioner.LongToken)next;
         long v = n.token - this.token;
         double d = Math.scalb((double)v, -64);
         return d > 0.0D?d:d + 1.0D;
      }

      public Token increaseSlightly() {
         return new Murmur3Partitioner.LongToken(this.token + 1L);
      }
   }
}
