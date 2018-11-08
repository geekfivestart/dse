package org.apache.cassandra.dht;

import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

public class RandomPartitioner implements IPartitioner {
   public static final BigInteger ZERO = new BigInteger("0");
   public static final RandomPartitioner.BigIntegerToken MINIMUM = new RandomPartitioner.BigIntegerToken("-1");
   public static final BigInteger MAXIMUM = (new BigInteger("2")).pow(127);
   private static final Supplier<MessageDigest> localMD5Digest = HashingUtils.newThreadLocalMessageDigest("MD5");
   private static final int HEAP_SIZE = (int)ObjectSizes.measureDeep(new RandomPartitioner.BigIntegerToken(hashToBigInteger(ByteBuffer.allocate(1))));
   public static final RandomPartitioner instance = new RandomPartitioner();
   public static final AbstractType<?> partitionOrdering;
   private final Splitter splitter = new Splitter(this);
   private static final Token.TokenFactory tokenFactory;

   public RandomPartitioner() {
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      return new CachedHashDecoratedKey(this.getToken(key), key);
   }

   public Token midpoint(Token ltoken, Token rtoken) {
      BigInteger left = ltoken.equals(MINIMUM)?ZERO:(BigInteger)((RandomPartitioner.BigIntegerToken)ltoken).token;
      BigInteger right = rtoken.equals(MINIMUM)?ZERO:(BigInteger)((RandomPartitioner.BigIntegerToken)rtoken).token;
      Pair<BigInteger, Boolean> midpair = FBUtilities.midpoint(left, right, 127);
      return new RandomPartitioner.BigIntegerToken((BigInteger)midpair.left);
   }

   public Token split(Token ltoken, Token rtoken, double ratioToLeft) {
      assert ratioToLeft >= 0.0D && ratioToLeft <= 1.0D;

      BigDecimal left = ltoken.equals(MINIMUM)?BigDecimal.ZERO:new BigDecimal((BigInteger)((RandomPartitioner.BigIntegerToken)ltoken).token);
      BigDecimal right = rtoken.equals(MINIMUM)?BigDecimal.ZERO:new BigDecimal((BigInteger)((RandomPartitioner.BigIntegerToken)rtoken).token);
      BigDecimal ratio = BigDecimal.valueOf(ratioToLeft);
      BigInteger newToken;
      if(left.compareTo(right) < 0) {
         newToken = right.subtract(left).multiply(ratio).add(left).toBigInteger();
      } else {
         BigDecimal max = new BigDecimal(MAXIMUM);
         newToken = max.add(right).subtract(left).multiply(ratio).add(left).toBigInteger().mod(MAXIMUM);
      }

      assert isValidToken(newToken) : "Invalid tokens from split";

      return new RandomPartitioner.BigIntegerToken(newToken);
   }

   public RandomPartitioner.BigIntegerToken getMinimumToken() {
      return MINIMUM;
   }

   public RandomPartitioner.BigIntegerToken getRandomToken() {
      BigInteger token = hashToBigInteger(GuidGenerator.guidAsBytes());
      if(token.signum() == -1) {
         token = token.multiply(BigInteger.valueOf(-1L));
      }

      return new RandomPartitioner.BigIntegerToken(token);
   }

   public RandomPartitioner.BigIntegerToken getRandomToken(Random random) {
      BigInteger token = hashToBigInteger(GuidGenerator.guidAsBytes(random, "host/127.0.0.1", 0L));
      if(token.signum() == -1) {
         token = token.multiply(BigInteger.valueOf(-1L));
      }

      return new RandomPartitioner.BigIntegerToken(token);
   }

   private static boolean isValidToken(BigInteger token) {
      return token.compareTo(ZERO) >= 0 && token.compareTo(MAXIMUM) <= 0;
   }

   public Token.TokenFactory getTokenFactory() {
      return tokenFactory;
   }

   public boolean preservesOrder() {
      return false;
   }

   public RandomPartitioner.BigIntegerToken getToken(ByteBuffer key) {
      return key.remaining() == 0?MINIMUM:new RandomPartitioner.BigIntegerToken(hashToBigInteger(key));
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
            BigInteger ri = MAXIMUM;
            BigDecimal r = new BigDecimal(ri);
            Token start = (Token)i.next();
            BigInteger ti = (BigInteger)((RandomPartitioner.BigIntegerToken)start).token;

            float x;
            for(BigInteger tim1 = ti; i.hasNext(); tim1 = ti) {
               Token t = (Token)i.next();
               ti = (BigInteger)((RandomPartitioner.BigIntegerToken)t).token;
               x = (new BigDecimal(ti.subtract(tim1).add(ri).mod(ri))).divide(r).floatValue();
               ownerships.put(t, Float.valueOf(x));
            }

            x = (new BigDecimal(((BigInteger)((RandomPartitioner.BigIntegerToken)start).token).subtract(ti).add(ri).mod(ri))).divide(r).floatValue();
            ownerships.put(start, Float.valueOf(x));
         }

         return ownerships;
      }
   }

   public Token getMaximumToken() {
      return new RandomPartitioner.BigIntegerToken(MAXIMUM);
   }

   public AbstractType<?> getTokenValidator() {
      return IntegerType.instance;
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
      return (BigInteger)((RandomPartitioner.BigIntegerToken)token).token;
   }

   private static BigInteger hashToBigInteger(ByteBuffer data) {
      MessageDigest messageDigest = (MessageDigest)localMD5Digest.get();
      if(data.hasArray()) {
         messageDigest.update(data.array(), data.arrayOffset() + data.position(), data.remaining());
      } else {
         messageDigest.update(data.duplicate());
      }

      return (new BigInteger(messageDigest.digest())).abs();
   }

   static {
      partitionOrdering = new PartitionerDefinedOrder(instance);
      tokenFactory = new Token.TokenFactory() {
         public ByteBuffer toByteArray(Token token) {
            RandomPartitioner.BigIntegerToken bigIntegerToken = (RandomPartitioner.BigIntegerToken)token;
            return ByteBuffer.wrap(((BigInteger)bigIntegerToken.token).toByteArray());
         }

         public Token fromByteArray(ByteBuffer bytes) {
            return new RandomPartitioner.BigIntegerToken(new BigInteger(ByteBufferUtil.getArray(bytes)));
         }

         public String toString(Token token) {
            RandomPartitioner.BigIntegerToken bigIntegerToken = (RandomPartitioner.BigIntegerToken)token;
            return ((BigInteger)bigIntegerToken.token).toString();
         }

         public void validate(String token) throws ConfigurationException {
            try {
               if(!RandomPartitioner.isValidToken(new BigInteger(token))) {
                  throw new ConfigurationException("Token must be >= 0 and <= 2**127");
               }
            } catch (NumberFormatException var3) {
               throw new ConfigurationException(var3.getMessage());
            }
         }

         public Token fromString(String string) {
            return new RandomPartitioner.BigIntegerToken(new BigInteger(string));
         }
      };
   }

   public static class BigIntegerToken extends ComparableObjectToken<BigInteger> {
      static final long serialVersionUID = -5833589141319293006L;

      public BigIntegerToken(BigInteger token) {
         super(token);
      }

      @VisibleForTesting
      public BigIntegerToken(String token) {
         this(new BigInteger(token));
      }

      public ByteSource asByteComparableSource() {
         return IntegerType.instance.asByteComparableSource(RandomPartitioner.tokenFactory.toByteArray(this));
      }

      public IPartitioner getPartitioner() {
         return RandomPartitioner.instance;
      }

      public long getHeapSize() {
         return (long)RandomPartitioner.HEAP_SIZE;
      }

      public Token increaseSlightly() {
         return new RandomPartitioner.BigIntegerToken(((BigInteger)this.token).add(BigInteger.ONE));
      }

      public double size(Token next) {
         RandomPartitioner.BigIntegerToken n = (RandomPartitioner.BigIntegerToken)next;
         BigInteger v = ((BigInteger)n.token).subtract((BigInteger)this.token);
         double d = Math.scalb(v.doubleValue(), -127);
         return d > 0.0D?d:d + 1.0D;
      }
   }
}
