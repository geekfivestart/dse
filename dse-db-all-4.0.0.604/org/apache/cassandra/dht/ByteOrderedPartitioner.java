package org.apache.cassandra.dht;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.ArrayUtils;

public class ByteOrderedPartitioner implements IPartitioner {
   public static final ByteOrderedPartitioner.BytesToken MINIMUM;
   public static final ByteOrderedPartitioner.BytesToken MAXIMUM;
   public static final BigInteger BYTE_MASK;
   private static final long EMPTY_SIZE;
   public static final ByteOrderedPartitioner instance;
   private final Optional<Splitter> splitter = Optional.of(new Splitter(this));
   private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
      public ByteBuffer toByteArray(Token token) {
         ByteOrderedPartitioner.BytesToken bytesToken = (ByteOrderedPartitioner.BytesToken)token;
         return ByteBuffer.wrap(bytesToken.token);
      }

      public Token fromByteArray(ByteBuffer bytes) {
         return new ByteOrderedPartitioner.BytesToken(bytes);
      }

      public String toString(Token token) {
         ByteOrderedPartitioner.BytesToken bytesToken = (ByteOrderedPartitioner.BytesToken)token;
         return Hex.bytesToHex(bytesToken.token);
      }

      public void validate(String token) throws ConfigurationException {
         try {
            if(token.length() % 2 == 1) {
               token = "0" + token;
            }

            Hex.hexToBytes(token);
         } catch (NumberFormatException var3) {
            throw new ConfigurationException("Token " + token + " contains non-hex digits");
         }
      }

      public Token fromString(String string) {
         if(string.length() % 2 == 1) {
            string = "0" + string;
         }

         return new ByteOrderedPartitioner.BytesToken(Hex.hexToBytes(string));
      }
   };

   public ByteOrderedPartitioner() {
   }

   public ByteOrderedPartitioner.BytesToken getToken(ByteBuffer key) {
      return key.remaining() == 0?MINIMUM:new ByteOrderedPartitioner.BytesToken(key);
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      return new BufferDecoratedKey(this.getToken(key), key);
   }

   public ByteOrderedPartitioner.BytesToken midpoint(Token lt, Token rt) {
      ByteOrderedPartitioner.BytesToken ltoken = (ByteOrderedPartitioner.BytesToken)lt;
      ByteOrderedPartitioner.BytesToken rtoken = (ByteOrderedPartitioner.BytesToken)rt;
      int sigbytes = Math.max(ltoken.token.length, rtoken.token.length);
      BigInteger left = this.bigForBytes(ltoken.token, sigbytes);
      BigInteger right = this.bigForBytes(rtoken.token, sigbytes);
      Pair<BigInteger, Boolean> midpair = FBUtilities.midpoint(left, right, 8 * sigbytes);
      return new ByteOrderedPartitioner.BytesToken(this.bytesForBig((BigInteger)midpair.left, sigbytes, ((Boolean)midpair.right).booleanValue()));
   }

   public Token split(Token lt, Token rt, double ratioToLeft) {
      assert ratioToLeft >= 0.0D && ratioToLeft <= 1.0D;

      if(ratioToLeft == 0.0D) {
         return lt;
      } else if(ratioToLeft == 1.0D) {
         return rt;
      } else {
         ByteOrderedPartitioner.BytesToken ltoken = (ByteOrderedPartitioner.BytesToken)lt;
         ByteOrderedPartitioner.BytesToken rtoken = (ByteOrderedPartitioner.BytesToken)rt;
         int sigbytes = Math.max(ltoken.token.length, rtoken.token.length);
         BigInteger leftBytes = this.bigForBytes(ltoken.token, sigbytes);
         BigDecimal left = new BigDecimal(leftBytes);
         BigInteger rightBytes = this.bigForBytes(rtoken.token, sigbytes);
         BigDecimal right = new BigDecimal(rightBytes);
         BigDecimal ratio = BigDecimal.valueOf(ratioToLeft);
         BigDecimal splitPoint;
         BigInteger newToken;
         if(left.compareTo(right) < 0) {
            splitPoint = right.subtract(left).multiply(ratio).add(left);
         } else {
            newToken = BigInteger.ONE.shiftLeft(8 * sigbytes);
            BigDecimal max = new BigDecimal(newToken);
            splitPoint = max.add(right).subtract(left).multiply(ratio).add(left);
            if(splitPoint.compareTo(max) >= 0) {
               splitPoint = splitPoint.subtract(max);
            }
         }

         for(newToken = splitPoint.toBigInteger(); newToken.equals(leftBytes) || newToken.equals(rightBytes); newToken = splitPoint.toBigInteger()) {
            ++sigbytes;
            leftBytes = leftBytes.shiftLeft(8);
            rightBytes = rightBytes.shiftLeft(8);
            splitPoint = splitPoint.multiply(BigDecimal.valueOf(256L));
         }

         return new ByteOrderedPartitioner.BytesToken(this.bytesForBig(newToken, sigbytes, false));
      }
   }

   private BigInteger bigForBytes(byte[] bytes, int sigbytes) {
      byte[] b;
      if(sigbytes != bytes.length) {
         b = Arrays.copyOf(bytes, sigbytes);
      } else {
         b = bytes;
      }

      return new BigInteger(1, b);
   }

   public Optional<Splitter> splitter() {
      return this.splitter;
   }

   private byte[] bytesForBig(BigInteger big, int sigbytes, boolean remainder) {
      byte[] bytes = new byte[sigbytes + (remainder?1:0)];
      if(remainder) {
         bytes[sigbytes] = (byte)(bytes[sigbytes] | 128);
      }

      for(int i = 0; i < sigbytes; ++i) {
         int maskpos = 8 * (sigbytes - (i + 1));
         bytes[i] = (byte)(big.and(BYTE_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() & 255);
      }

      return bytes;
   }

   public ByteOrderedPartitioner.BytesToken getMinimumToken() {
      return MINIMUM;
   }

   public ByteOrderedPartitioner.BytesToken getMaximumToken() {
      return MAXIMUM;
   }

   public ByteOrderedPartitioner.BytesToken getRandomToken() {
      return this.getRandomToken(ThreadLocalRandom.current());
   }

   public ByteOrderedPartitioner.BytesToken getRandomToken(Random random) {
      byte[] buffer = new byte[16];
      random.nextBytes(buffer);
      return new ByteOrderedPartitioner.BytesToken(buffer);
   }

   public Token.TokenFactory getTokenFactory() {
      return this.tokenFactory;
   }

   public boolean preservesOrder() {
      return true;
   }

   public Map<Token, Float> describeOwnership(List<Token> sortedTokens) {
      Map<Token, Float> allTokens = Maps.newHashMapWithExpectedSize(sortedTokens.size());
      List<Range<Token>> sortedRanges = new ArrayList(sortedTokens.size());
      Token lastToken = (Token)sortedTokens.get(sortedTokens.size() - 1);

      Iterator var5;
      Token node;
      for(var5 = sortedTokens.iterator(); var5.hasNext(); lastToken = node) {
         node = (Token)var5.next();
         allTokens.put(node, new Float(0.0D));
         sortedRanges.add(new Range(lastToken, node));
      }

      var5 = Schema.instance.getKeyspaces().iterator();

      while(var5.hasNext()) {
         String ks = (String)var5.next();
         Iterator var7 = Schema.instance.getTablesAndViews(ks).iterator();

         while(var7.hasNext()) {
            TableMetadata cfmd = (TableMetadata)var7.next();
            Iterator var9 = sortedRanges.iterator();

            while(var9.hasNext()) {
               Range<Token> r = (Range)var9.next();
               allTokens.put(r.right, Float.valueOf(((Float)allTokens.get(r.right)).floatValue() + (float)StorageService.instance.getSplits(ks, cfmd.name, r, 1).size()));
            }
         }
      }

      Float total = new Float(0.0D);

      Iterator var13;
      Float f;
      for(var13 = allTokens.values().iterator(); var13.hasNext(); total = Float.valueOf(total.floatValue() + f.floatValue())) {
         f = (Float)var13.next();
      }

      var13 = allTokens.entrySet().iterator();

      while(var13.hasNext()) {
         Entry<Token, Float> row = (Entry)var13.next();
         allTokens.put(row.getKey(), Float.valueOf(((Float)row.getValue()).floatValue() / total.floatValue()));
      }

      return allTokens;
   }

   public AbstractType<?> getTokenValidator() {
      return BytesType.instance;
   }

   public AbstractType<?> partitionOrdering() {
      return BytesType.instance;
   }

   static {
      MINIMUM = new ByteOrderedPartitioner.BytesToken(ArrayUtils.EMPTY_BYTE_ARRAY);
      MAXIMUM = new ByteOrderedPartitioner.MaxBytesToken();
      BYTE_MASK = new BigInteger("255");
      EMPTY_SIZE = ObjectSizes.measure(MINIMUM);
      instance = new ByteOrderedPartitioner();
   }

   public static class MaxBytesToken extends ByteOrderedPartitioner.BytesToken {
      public MaxBytesToken() {
         super(ArrayUtils.EMPTY_BYTE_ARRAY);
      }

      public int compareTo(Token other) {
         return other instanceof ByteOrderedPartitioner.MaxBytesToken?0:1;
      }

      public boolean equals(Object obj) {
         return obj instanceof ByteOrderedPartitioner.MaxBytesToken;
      }

      public ByteSource asByteComparableSource() {
         return ByteSource.max();
      }
   }

   public static class BytesToken extends Token {
      static final long serialVersionUID = -2630749093733680626L;
      final byte[] token;

      public BytesToken(ByteBuffer token) {
         this(ByteBufferUtil.getArray(token));
      }

      public BytesToken(byte[] token) {
         this.token = token;
      }

      public String toString() {
         return Hex.bytesToHex(this.token);
      }

      public int compareTo(Token other) {
         if(other instanceof ByteOrderedPartitioner.MaxBytesToken) {
            return -other.compareTo(this);
         } else {
            ByteOrderedPartitioner.BytesToken o = (ByteOrderedPartitioner.BytesToken)other;
            return FBUtilities.compareUnsigned(this.token, o.token, 0, 0, this.token.length, o.token.length);
         }
      }

      public ByteSource asByteComparableSource() {
         return ByteSource.of(this.token);
      }

      public int hashCode() {
         int prime = true;
         return 31 + Arrays.hashCode(this.token);
      }

      public boolean equals(Object obj) {
         if(this == obj) {
            return true;
         } else if(obj instanceof ByteOrderedPartitioner.MaxBytesToken) {
            return obj.equals(this);
         } else if(!(obj instanceof ByteOrderedPartitioner.BytesToken)) {
            return false;
         } else {
            ByteOrderedPartitioner.BytesToken other = (ByteOrderedPartitioner.BytesToken)obj;
            return Arrays.equals(this.token, other.token);
         }
      }

      public IPartitioner getPartitioner() {
         return ByteOrderedPartitioner.instance;
      }

      public long getHeapSize() {
         return ByteOrderedPartitioner.EMPTY_SIZE + ObjectSizes.sizeOfArray(this.token);
      }

      public Object getTokenValue() {
         return this.token;
      }

      public double size(Token next) {
         byte[] other = ((ByteOrderedPartitioner.BytesToken)next).token;
         int shift = 0;

         float var10001;
         double sz;
         for(sz = 0.0D; shift < this.token.length && shift < other.length; sz += (double)Math.scalb(var10001, shift * -8)) {
            var10001 = (float)((other[shift] & 255) - (this.token[shift] & 255));
            ++shift;
         }

         while(shift < this.token.length) {
            var10001 = (float)(-this.token[shift] & 255);
            ++shift;
            sz += (double)Math.scalb(var10001, shift * -8);
         }

         while(shift < other.length) {
            var10001 = (float)(other[shift] & 255);
            ++shift;
            sz += (double)Math.scalb(var10001, shift * -8);
         }

         if(sz < 0.0D || sz == 0.0D && this.token.length >= other.length) {
            ++sz;
         }

         return sz;
      }

      public Token increaseSlightly() {
         throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.", new Object[]{this.getClass().getSimpleName()}));
      }
   }
}
