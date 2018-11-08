package com.datastax.bdp.dht;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionPosition.Kind;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringEscapeUtils;

public final class DHTUtil {
   private static final Splitter TOKEN_RANGE_SPLITTER = Splitter.on(Pattern.compile("(?<!\\\\),")).trimResults();
   private static final Splitter KEY_PARTS_SPLITTER = Splitter.on(Pattern.compile("(?<!\\\\)\\|")).trimResults();
   private static final Joiner KEY_PARTS_JOINER = Joiner.on('|');
   private static final Joiner FILTER_TERMS_JOINER = Joiner.on(" AND ");
   private static final String LEFT_INTERVAL_MARK_PATTERN = "(\\(|\\[)";
   private static final String RIGHT_INTERVAL_MARK_PATTERN = "(\\)|\\])";
   private static final String CLOSED_LEFT_INTERVAL = "[";
   private static final String OPEN_LEFT_INTERVAL = "(";
   private static final String CLOSED_RIGHT_INTERVAL = "]";
   private static final String OPEN_RIGHT_INTERVAL = ")";

   private DHTUtil() {
   }

   public static Set<Range<Token>> deoverlapTokenRanges(List<Range<Token>> ranges) {
      if(ranges.size() < 2) {
         return Sets.newHashSet(ranges);
      } else {
         Queue<Range<Token>> input = sortedRangesQueue(ranges);
         HashSet output = Sets.newHashSet();

         while(!input.isEmpty()) {
            output.add(mergeInitialRanges(input));
         }

         return output;
      }
   }

   public static String encodePartitionKey(List<Pair<String, String>> components) {
      List<String> keys = Lists.transform(components, (component) -> {
         return (String)component.right;
      });
      return KEY_PARTS_JOINER.join(keys);
   }

   public static String encodeFilterQueryFromPartitionKey(List<Pair<String, String>> components) {
      List<String> terms = Lists.transform(components, (component) -> {
         return (String)component.left + ':' + escapeSolrQueryChars((String)component.right);
      });
      return FILTER_TERMS_JOINER.join(terms);
   }

   public static Token decodePartitionKeyAsToken(AbstractType<?> validator, IPartitioner partitioner, String key) {
      if(!(validator instanceof CompositeType)) {
         String unescapedKey = StringEscapeUtils.unescapeJava(key);
         return partitioner.getToken(validator.fromString(unescapedKey));
      } else {
         CompositeType compositeValidator = (CompositeType)validator;
         List<AbstractType<?>> compositeComponents = compositeValidator.getComponents();
         List<String> keyParts = KEY_PARTS_SPLITTER.splitToList(key);
         if(compositeComponents.size() != keyParts.size()) {
            throw new IllegalArgumentException("Cannot convert to token partition key: " + key + ", with validator: " + validator);
         } else {
            List<ByteBuffer> buffers = new ArrayList();
            Iterator<AbstractType<?>> componentsIt = compositeComponents.iterator();
            Iterator partsIt = keyParts.iterator();

            while(componentsIt.hasNext() && partsIt.hasNext()) {
               String unescapedKey = StringEscapeUtils.unescapeJava((String)partsIt.next());
               buffers.add(((AbstractType)componentsIt.next()).fromString(unescapedKey));
            }

            return partitioner.getToken(CompositeType.build((ByteBuffer[])buffers.toArray(new ByteBuffer[buffers.size()])));
         }
      }
   }

   public static String encodeInterval(PartitionPosition left, PartitionPosition right) {
      String leftIntervalType = left.kind() == Kind.MAX_BOUND?"(":"[";
      String rightIntervalType = right.kind() == Kind.MIN_BOUND?")":"]";
      Object leftValue = left.getToken().getTokenValue();
      Object rightValue = right.getToken().getTokenValue();
      return String.format("%s%s,%s%s", new Object[]{leftIntervalType, leftValue, rightValue, rightIntervalType});
   }

   public static Range<Token> decodeInterval(IPartitioner partitioner, String interval) {
      TokenFactory tokenFactory = partitioner.getTokenFactory();
      List<String> intervalParts = TOKEN_RANGE_SPLITTER.splitToList(interval);
      if(intervalParts.size() == 2) {
         String leftValue = (String)intervalParts.get(0);
         boolean isClosedOnLeft = leftValue.startsWith("[");
         leftValue = leftValue.replaceAll("(\\(|\\[)", "");
         Token leftToken = tokenFactory.fromString(leftValue);
         if(!leftToken.isMinimum() && isClosedOnLeft) {
            leftToken = tokenFactory.fromString((new BigInteger(leftValue)).subtract(BigInteger.ONE).toString());
         }

         String rightValue = (String)intervalParts.get(1);
         boolean isOpenOnRight = rightValue.endsWith(")");
         rightValue = rightValue.replaceAll("(\\)|\\])", "");
         Token rightToken = tokenFactory.fromString(rightValue);
         if(!rightToken.isMinimum() && isOpenOnRight) {
            rightToken = tokenFactory.fromString((new BigInteger(rightValue)).subtract(BigInteger.ONE).toString());
         }

         return new Range(leftToken, rightToken);
      } else {
         throw new IllegalArgumentException(String.format("Invalid token range: %s", new Object[]{interval}));
      }
   }

   private static Queue<Range<Token>> sortedRangesQueue(List<Range<Token>> ranges) {
      ranges.sort(new DHTUtil.LeftBoundRangeComparator());
      return new ArrayDeque(ranges);
   }

   private static Range<Token> mergeInitialRanges(Queue<Range<Token>> queue) {
      assert !queue.isEmpty() : "Queue must not be empty";

      Range<Token> current = (Range)queue.poll();
      Token min = (Token)current.left;

      Token max;
      for(max = (Token)current.right; !queue.isEmpty() && ((Token)((Range)queue.peek()).left).compareTo(max) <= 0; max = (Token)((Range)queue.poll()).right) {
         ;
      }

      return new Range(min, max);
   }

   private static String escapeSolrQueryChars(String s) {
      StringBuilder sb = new StringBuilder();

      for(int i = 0; i < s.length(); ++i) {
         char c = s.charAt(i);
         if(c == 92 || c == 43 || c == 45 || c == 33 || c == 40 || c == 41 || c == 58 || c == 94 || c == 91 || c == 93 || c == 34 || c == 123 || c == 125 || c == 126 || c == 42 || c == 63 || c == 124 || c == 38 || c == 59 || c == 47 || Character.isWhitespace(c)) {
            sb.append('\\');
         }

         sb.append(c);
      }

      return sb.toString();
   }

   private static final class LeftBoundRangeComparator implements Comparator<Range<Token>> {
      private LeftBoundRangeComparator() {
      }

      public int compare(Range<Token> first, Range<Token> second) {
         int result = ((Token)first.left).compareTo(second.left);
         if(result == 0) {
            result = ((Token)first.right).compareTo(second.right);
         }

         return result;
      }
   }
}
