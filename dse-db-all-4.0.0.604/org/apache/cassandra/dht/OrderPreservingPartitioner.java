package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

public class OrderPreservingPartitioner implements IPartitioner {
   private static final String rndchars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
   public static final OrderPreservingPartitioner.StringToken MINIMUM = new OrderPreservingPartitioner.StringToken("");
   public static final BigInteger CHAR_MASK = new BigInteger("65535");
   private static final long EMPTY_SIZE;
   public static final OrderPreservingPartitioner instance;
   private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
      public ByteBuffer toByteArray(Token token) {
         OrderPreservingPartitioner.StringToken stringToken = (OrderPreservingPartitioner.StringToken)token;
         return ByteBufferUtil.bytes((String)stringToken.token);
      }

      public Token fromByteArray(ByteBuffer bytes) {
         try {
            return new OrderPreservingPartitioner.StringToken(ByteBufferUtil.string(bytes));
         } catch (CharacterCodingException var3) {
            throw new RuntimeException(var3);
         }
      }

      public String toString(Token token) {
         OrderPreservingPartitioner.StringToken stringToken = (OrderPreservingPartitioner.StringToken)token;
         return (String)stringToken.token;
      }

      public void validate(String token) throws ConfigurationException {
         if(token.contains(VersionedValue.DELIMITER_STR)) {
            throw new ConfigurationException("Tokens may not contain the character " + VersionedValue.DELIMITER_STR);
         }
      }

      public Token fromString(String string) {
         return new OrderPreservingPartitioner.StringToken(string);
      }
   };

   public OrderPreservingPartitioner() {
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      return new CachedHashDecoratedKey(this.getToken(key), key);
   }

   public OrderPreservingPartitioner.StringToken midpoint(Token ltoken, Token rtoken) {
      int sigchars = Math.max(((String)((OrderPreservingPartitioner.StringToken)ltoken).token).length(), ((String)((OrderPreservingPartitioner.StringToken)rtoken).token).length());
      BigInteger left = bigForString((String)((OrderPreservingPartitioner.StringToken)ltoken).token, sigchars);
      BigInteger right = bigForString((String)((OrderPreservingPartitioner.StringToken)rtoken).token, sigchars);
      Pair<BigInteger, Boolean> midpair = FBUtilities.midpoint(left, right, 16 * sigchars);
      return new OrderPreservingPartitioner.StringToken(this.stringForBig((BigInteger)midpair.left, sigchars, ((Boolean)midpair.right).booleanValue()));
   }

   public Token split(Token left, Token right, double ratioToLeft) {
      throw new UnsupportedOperationException();
   }

   private static BigInteger bigForString(String str, int sigchars) {
      assert str.length() <= sigchars;

      BigInteger big = BigInteger.ZERO;

      for(int i = 0; i < str.length(); ++i) {
         int charpos = 16 * (sigchars - (i + 1));
         BigInteger charbig = BigInteger.valueOf((long)(str.charAt(i) & '\uffff'));
         big = big.or(charbig.shiftLeft(charpos));
      }

      return big;
   }

   private String stringForBig(BigInteger big, int sigchars, boolean remainder) {
      char[] chars = new char[sigchars + (remainder?1:0)];
      if(remainder) {
         chars[sigchars] |= '耀';
      }

      for(int i = 0; i < sigchars; ++i) {
         int maskpos = 16 * (sigchars - (i + 1));
         chars[i] = (char)(big.and(CHAR_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() & '\uffff');
      }

      return new String(chars);
   }

   public OrderPreservingPartitioner.StringToken getMinimumToken() {
      return MINIMUM;
   }

   public OrderPreservingPartitioner.StringToken getRandomToken() {
      return this.getRandomToken(ThreadLocalRandom.current());
   }

   public OrderPreservingPartitioner.StringToken getRandomToken(Random random) {
      StringBuilder buffer = new StringBuilder();

      for(int j = 0; j < 16; ++j) {
         buffer.append("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".charAt(random.nextInt("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".length())));
      }

      return new OrderPreservingPartitioner.StringToken(buffer.toString());
   }

   public Token.TokenFactory getTokenFactory() {
      return this.tokenFactory;
   }

   public boolean preservesOrder() {
      return true;
   }

   public OrderPreservingPartitioner.StringToken getToken(ByteBuffer key) {
      String skey;
      try {
         skey = ByteBufferUtil.string(key);
      } catch (CharacterCodingException var4) {
         skey = ByteBufferUtil.bytesToHex(key);
      }

      return new OrderPreservingPartitioner.StringToken(skey);
   }

   public Map<Token, Float> describeOwnership(List<Token> sortedTokens) {
      Map<Token, Float> allTokens = new HashMap();
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
               allTokens.put(r.right, Float.valueOf(((Float)allTokens.get(r.right)).floatValue() + (float)StorageService.instance.getSplits(ks, cfmd.name, r, cfmd.params.minIndexInterval).size()));
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
      return UTF8Type.instance;
   }

   public AbstractType<?> partitionOrdering() {
      return UTF8Type.instance;
   }

   static {
      EMPTY_SIZE = ObjectSizes.measure(MINIMUM);
      instance = new OrderPreservingPartitioner();
   }

   public static class StringToken extends ComparableObjectToken<String> {
      static final long serialVersionUID = 5464084395277974963L;

      public StringToken(String token) {
         super(token);
      }

      public IPartitioner getPartitioner() {
         return OrderPreservingPartitioner.instance;
      }

      public long getHeapSize() {
         return OrderPreservingPartitioner.EMPTY_SIZE + ObjectSizes.sizeOf((String)this.token);
      }

      public ByteSource asByteComparableSource() {
         return ByteSource.of((String)this.token);
      }
   }
}
