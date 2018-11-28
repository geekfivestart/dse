package org.apache.cassandra.index.sasi.memory;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.Node;
import com.googlecode.concurrenttrees.radix.node.concrete.SmartArrayBasedNodeFactory;
import com.googlecode.concurrenttrees.suffix.ConcurrentSuffixTree;
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrieMemIndex extends MemIndex {
   private static final Logger logger = LoggerFactory.getLogger(TrieMemIndex.class);
   private final TrieMemIndex.ConcurrentTrie index;

   public TrieMemIndex(AbstractType<?> keyValidator, ColumnIndex columnIndex) {
      super(keyValidator, columnIndex);
      switch (columnIndex.getMode().mode) {
         case CONTAINS: {
            this.index = new ConcurrentSuffixTrie(columnIndex.getDefinition());
            break;
         }
         case PREFIX: {
            this.index = new ConcurrentPrefixTrie(columnIndex.getDefinition());
            break;
         }
         default: {
            throw new IllegalStateException("Unsupported mode: " + (Object)((Object)columnIndex.getMode().mode));
         }
      }
   }

   public long add(DecoratedKey key, ByteBuffer value) {
      AbstractAnalyzer analyzer = this.columnIndex.getAnalyzer();
      analyzer.reset(value.duplicate());
      long size = 0L;

      while(analyzer.hasNext()) {
         ByteBuffer term = analyzer.next();
         if(term.remaining() >= 1024) {
            logger.info("Can't add term of column {} to index for key: {}, term size {}, max allowed size {}, use analyzed = true (if not yet set) for that column.", new Object[]{this.columnIndex.getColumnName(), this.keyValidator.getString(key.getKey()), FBUtilities.prettyPrintMemory((long)term.remaining()), FBUtilities.prettyPrintMemory(1024L)});
         } else {
            size += this.index.add(this.columnIndex.getValidator().getString(term), key);
         }
      }

      return size;
   }

   public RangeIterator<Long, Token> search(Expression expression) {
      return this.index.search(expression);
   }

   private static class SizeEstimatingNodeFactory extends SmartArrayBasedNodeFactory {
      private static final FastThreadLocal<Long> updateSize = new FastThreadLocal<Long>() {
         protected Long initialValue() throws Exception {
            return Long.valueOf(0L);
         }
      };

      private SizeEstimatingNodeFactory() {
      }

      public Node createNode(CharSequence edgeCharacters, Object value, List<Node> childNodes, boolean isRoot) {
         Node node = super.createNode(edgeCharacters, value, childNodes, isRoot);
         updateSize.set(Long.valueOf(((Long)updateSize.get()).longValue() + this.measure(node)));
         return node;
      }

      public long currentUpdateSize() {
         return ((Long)updateSize.get()).longValue();
      }

      public void reset() {
         updateSize.set(Long.valueOf(0L));
      }

      private long measure(Node node) {
         long overhead = 24L;
         overhead += (long)(24 + node.getIncomingEdge().length() * 2);
         if(node.getOutgoingEdges() != null) {
            overhead += 16L;
            overhead += (long)(24 * node.getOutgoingEdges().size());
         }

         return overhead;
      }
   }

   protected static class ConcurrentSuffixTrie extends TrieMemIndex.ConcurrentTrie {
      private final ConcurrentSuffixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

      private ConcurrentSuffixTrie(ColumnMetadata column) {
         super(column);
         this.trie = new ConcurrentSuffixTree(NODE_FACTORY);
      }

      public ConcurrentSkipListSet<DecoratedKey> get(String value) {
         return (ConcurrentSkipListSet)this.trie.getValueForExactKey(value);
      }

      public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys) {
         return (ConcurrentSkipListSet)this.trie.putIfAbsent(value, newKeys);
      }

      public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(Expression.Op operator, String value) {
         switch (operator) {
            case EQ:
            case MATCH: {
               ConcurrentSkipListSet keys = (ConcurrentSkipListSet)this.trie.getValueForExactKey((CharSequence)value);
               return keys == null ? Collections.emptyList() : Collections.singletonList(keys);
            }
            case SUFFIX: {
               return this.trie.getValuesForKeysEndingWith((CharSequence)value);
            }
            case PREFIX:
            case CONTAINS: {
               return this.trie.getValuesForKeysContaining((CharSequence)value);
            }
         }
         throw new UnsupportedOperationException(String.format("operation %s is not supported.", new Object[]{operator}));
      }
   }

   protected static class ConcurrentPrefixTrie extends TrieMemIndex.ConcurrentTrie {
      private final ConcurrentRadixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

      private ConcurrentPrefixTrie(ColumnMetadata column) {
         super(column);
         this.trie = new ConcurrentRadixTree(NODE_FACTORY);
      }

      public ConcurrentSkipListSet<DecoratedKey> get(String value) {
         return (ConcurrentSkipListSet)this.trie.getValueForExactKey(value);
      }

      public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys) {
         return (ConcurrentSkipListSet)this.trie.putIfAbsent(value, newKeys);
      }

      public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(Expression.Op operator, String value) {
         switch (operator) {
            case EQ:
            case MATCH: {
               ConcurrentSkipListSet keys = (ConcurrentSkipListSet)this.trie.getValueForExactKey((CharSequence)value);
               return keys == null ? Collections.emptyList() : Collections.singletonList(keys);
            }
            case PREFIX: {
               return this.trie.getValuesForKeysStartingWith((CharSequence)value);
            }
         }
         throw new UnsupportedOperationException(String.format("operation %s is not supported.", new Object[]{operator}));
      }
   }

   private abstract static class ConcurrentTrie {
      public static final TrieMemIndex.SizeEstimatingNodeFactory NODE_FACTORY = new TrieMemIndex.SizeEstimatingNodeFactory();
      protected final ColumnMetadata definition;

      public ConcurrentTrie(ColumnMetadata column) {
         this.definition = column;
      }

      public long add(String value, DecoratedKey key) {
         long overhead = 128L;
         ConcurrentSkipListSet<DecoratedKey> keys = this.get(value);
         if(keys == null) {
            ConcurrentSkipListSet<DecoratedKey> newKeys = new ConcurrentSkipListSet(DecoratedKey.comparator);
            keys = this.putIfAbsent(value, newKeys);
            if(keys == null) {
               overhead += (long)(128 + value.length());
               keys = newKeys;
            }
         }

         keys.add(key);
         overhead += NODE_FACTORY.currentUpdateSize();
         NODE_FACTORY.reset();
         return overhead;
      }

      public RangeIterator<Long, Token> search(Expression expression) {
         ByteBuffer prefix = expression.lower == null?null:expression.lower.value;
         Iterable<ConcurrentSkipListSet<DecoratedKey>> search = this.search(expression.getOp(), this.definition.cellValueType().getString(prefix));
         RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
         Iterator var5 = search.iterator();

         while(var5.hasNext()) {
            ConcurrentSkipListSet<DecoratedKey> keys = (ConcurrentSkipListSet)var5.next();
            if(!keys.isEmpty()) {
               builder.add(new KeyRangeIterator(keys));
            }
         }

         return builder.build();
      }

      protected abstract ConcurrentSkipListSet<DecoratedKey> get(String var1);

      protected abstract Iterable<ConcurrentSkipListSet<DecoratedKey>> search(Expression.Op var1, String var2);

      protected abstract ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String var1, ConcurrentSkipListSet<DecoratedKey> var2);
   }
}
