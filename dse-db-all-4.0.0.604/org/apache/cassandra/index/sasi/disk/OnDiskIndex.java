package org.apache.cassandra.index.sasi.disk;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.Term;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class OnDiskIndex implements Iterable<OnDiskIndex.DataTerm>, Closeable {
   public final Descriptor descriptor;
   protected final OnDiskIndexBuilder.Mode mode;
   protected final OnDiskIndexBuilder.TermSize termSize;
   protected final AbstractType<?> comparator;
   protected final MappedBuffer indexFile;
   protected final long indexSize;
   protected final boolean hasMarkedPartials;
   protected final Function<Long, DecoratedKey> keyFetcher;
   protected final String indexPath;
   protected final OnDiskIndex.PointerLevel[] levels;
   protected final OnDiskIndex.DataLevel dataLevel;
   protected final ByteBuffer minTerm;
   protected final ByteBuffer maxTerm;
   protected final ByteBuffer minKey;
   protected final ByteBuffer maxKey;

   public OnDiskIndex(File index, AbstractType<?> cmp, Function<Long, DecoratedKey> keyReader) {
      this.keyFetcher = keyReader;
      this.comparator = cmp;
      this.indexPath = index.getAbsolutePath();
      RandomAccessFile backingFile = null;

      try {
         backingFile = new RandomAccessFile(index, "r");
         this.descriptor = new Descriptor(backingFile.readUTF());
         this.termSize = OnDiskIndexBuilder.TermSize.of(backingFile.readShort());
         this.minTerm = ByteBufferUtil.readWithShortLength(backingFile);
         this.maxTerm = ByteBufferUtil.readWithShortLength(backingFile);
         this.minKey = ByteBufferUtil.readWithShortLength(backingFile);
         this.maxKey = ByteBufferUtil.readWithShortLength(backingFile);
         this.mode = OnDiskIndexBuilder.Mode.mode(backingFile.readUTF());
         this.hasMarkedPartials = backingFile.readBoolean();
         this.indexSize = backingFile.length();
         this.indexFile = new MappedBuffer(new ChannelProxy(this.indexPath, backingFile.getChannel()));
         this.indexFile.position(this.indexFile.getLong(this.indexSize - 8L));
         int numLevels = this.indexFile.getInt();
         this.levels = new OnDiskIndex.PointerLevel[numLevels];

         int blockCount;
         for(blockCount = 0; blockCount < this.levels.length; ++blockCount) {
            int blockCount = this.indexFile.getInt();
            this.levels[blockCount] = new OnDiskIndex.PointerLevel(this.indexFile.position(), blockCount);
            this.indexFile.position(this.indexFile.position() + (long)(blockCount * 8));
         }

         blockCount = this.indexFile.getInt();
         this.dataLevel = new OnDiskIndex.DataLevel(this.indexFile.position(), blockCount);
      } catch (IOException var11) {
         throw new FSReadError(var11, index);
      } finally {
         FileUtils.closeQuietly((Closeable)backingFile);
      }
   }

   public boolean hasMarkedPartials() {
      return this.hasMarkedPartials;
   }

   public OnDiskIndexBuilder.Mode mode() {
      return this.mode;
   }

   public ByteBuffer minTerm() {
      return this.minTerm;
   }

   public ByteBuffer maxTerm() {
      return this.maxTerm;
   }

   public ByteBuffer minKey() {
      return this.minKey;
   }

   public ByteBuffer maxKey() {
      return this.maxKey;
   }

   public OnDiskIndex.DataTerm min() {
      return (OnDiskIndex.DataTerm)((OnDiskIndex.DataBlock)this.dataLevel.getBlock(0)).getTerm(0);
   }

   public OnDiskIndex.DataTerm max() {
      OnDiskIndex.DataBlock block = (OnDiskIndex.DataBlock)this.dataLevel.getBlock(this.dataLevel.blockCount - 1);
      return (OnDiskIndex.DataTerm)block.getTerm(block.termCount() - 1);
   }

   public RangeIterator<Long, Token> search(Expression exp) {
      assert this.mode.supports(exp.getOp());

      if(exp.getOp() == Expression.Op.PREFIX && this.mode == OnDiskIndexBuilder.Mode.CONTAINS && !this.hasMarkedPartials) {
         throw new UnsupportedOperationException("prefix queries in CONTAINS mode are not supported by this index");
      } else if(exp.getOp() == Expression.Op.EQ) {
         OnDiskIndex.DataTerm term = this.getTerm(exp.lower.value);
         return term == null?null:term.getTokens();
      } else {
         Expression expression = exp.getOp() != Expression.Op.NOT_EQ?exp:(new Expression(exp)).setOp(Expression.Op.RANGE).setLower(new Expression.Bound(this.minTerm, true)).setUpper(new Expression.Bound(this.maxTerm, true)).addExclusion(exp.lower.value);
         List<ByteBuffer> exclusions = new ArrayList(expression.exclusions.size());
         Iterables.addAll(exclusions, (Iterable)expression.exclusions.stream().filter((exclusion) -> {
            return (expression.lower == null || this.comparator.compare(exclusion, expression.lower.value) >= 0) && (expression.upper == null || this.comparator.compare(exclusion, expression.upper.value) <= 0);
         }).collect(Collectors.toList()));
         Collections.sort(exclusions, this.comparator);
         if(exclusions.size() == 0) {
            return this.searchRange(expression);
         } else {
            List<Expression> ranges = new ArrayList(exclusions.size());
            Iterator<ByteBuffer> exclusionsIterator = exclusions.iterator();
            Expression.Bound min = expression.lower;

            Expression.Bound max;
            for(max = null; exclusionsIterator.hasNext(); min = max) {
               max = new Expression.Bound((ByteBuffer)exclusionsIterator.next(), false);
               ranges.add((new Expression(expression)).setOp(Expression.Op.RANGE).setLower(min).setUpper(max));
            }

            assert max != null;

            ranges.add((new Expression(expression)).setOp(Expression.Op.RANGE).setLower(max).setUpper(expression.upper));
            RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
            Iterator var9 = ranges.iterator();

            while(var9.hasNext()) {
               Expression e = (Expression)var9.next();
               RangeIterator<Long, Token> range = this.searchRange(e);
               if(range != null) {
                  builder.add(range);
               }
            }

            return builder.build();
         }
      }
   }

   private RangeIterator<Long, Token> searchRange(Expression range) {
      Expression.Bound lower = range.lower;
      Expression.Bound upper = range.upper;
      int lowerBlock = lower == null?0:this.getDataBlock(lower.value);
      int upperBlock = upper == null?this.dataLevel.blockCount - 1:(lower != null && this.comparator.compare(lower.value, upper.value) == 0?lowerBlock:this.getDataBlock(upper.value));
      return this.mode == OnDiskIndexBuilder.Mode.SPARSE && lowerBlock != upperBlock && upperBlock - lowerBlock > 1?this.searchRange(lowerBlock, lower, upperBlock, upper):this.searchPoint(lowerBlock, range);
   }

   private RangeIterator<Long, Token> searchRange(int lowerBlock, Expression.Bound lower, int upperBlock, Expression.Bound upper) {
      OnDiskBlock.SearchResult<OnDiskIndex.DataTerm> lowerPosition = lower == null?null:this.searchIndex(lower.value, lowerBlock);
      OnDiskBlock.SearchResult<OnDiskIndex.DataTerm> upperPosition = upper == null?null:this.searchIndex(upper.value, upperBlock);
      RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
      int firstFullBlockIdx = lowerBlock;
      int lastFullBlockIdx = upperBlock;
      OnDiskIndex.DataBlock block;
      int superBlockAlignedStart;
      if(lowerPosition != null && (lowerPosition.index > 0 || !lower.inclusive)) {
         block = (OnDiskIndex.DataBlock)this.dataLevel.getBlock(lowerBlock);
         superBlockAlignedStart = !lower.inclusive && lowerPosition.cmp == 0?lowerPosition.index + 1:lowerPosition.index;
         builder.add(block.getRange(superBlockAlignedStart, block.termCount()));
         firstFullBlockIdx = lowerBlock + 1;
      }

      int superBlockIdx;
      if(upperPosition != null) {
         block = (OnDiskIndex.DataBlock)this.dataLevel.getBlock(upperBlock);
         superBlockAlignedStart = block.termCount() - 1;
         if(upperPosition.index != superBlockAlignedStart || !upper.inclusive) {
            superBlockIdx = upperPosition.cmp >= 0 && (upperPosition.cmp != 0 || !upper.inclusive)?upperPosition.index:upperPosition.index + 1;
            builder.add(block.getRange(0, superBlockIdx));
            lastFullBlockIdx = upperBlock - 1;
         }
      }

      int totalSuperBlocks = (lastFullBlockIdx - firstFullBlockIdx) / 64;
      if(totalSuperBlocks == 0) {
         for(superBlockAlignedStart = firstFullBlockIdx; superBlockAlignedStart <= lastFullBlockIdx; ++superBlockAlignedStart) {
            builder.add(((OnDiskIndex.DataBlock)this.dataLevel.getBlock(superBlockAlignedStart)).getBlockIndex().iterator(this.keyFetcher));
         }

         return builder.build();
      } else {
         superBlockAlignedStart = firstFullBlockIdx == 0?0:(int)FBUtilities.align((long)firstFullBlockIdx, 64);

         for(superBlockIdx = firstFullBlockIdx; superBlockIdx < Math.min(superBlockAlignedStart, lastFullBlockIdx); ++superBlockIdx) {
            builder.add(this.getBlockIterator(superBlockIdx));
         }

         superBlockIdx = superBlockAlignedStart / 64;

         int lastCoveredBlock;
         for(lastCoveredBlock = 0; lastCoveredBlock < totalSuperBlocks - 1; ++lastCoveredBlock) {
            builder.add(this.dataLevel.getSuperBlock(superBlockIdx++).iterator());
         }

         lastCoveredBlock = superBlockIdx * 64;

         for(int offset = 0; offset <= lastFullBlockIdx - lastCoveredBlock; ++offset) {
            builder.add(this.getBlockIterator(lastCoveredBlock + offset));
         }

         return builder.build();
      }
   }

   private RangeIterator<Long, Token> searchPoint(int lowerBlock, Expression expression) {
      Iterator<OnDiskIndex.DataTerm> terms = new OnDiskIndex.TermIterator(lowerBlock, expression, OnDiskIndex.IteratorOrder.DESC);
      RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

      while(terms.hasNext()) {
         try {
            builder.add(((OnDiskIndex.DataTerm)terms.next()).getTokens());
         } finally {
            expression.checkpoint();
         }
      }

      return builder.build();
   }

   private RangeIterator<Long, Token> getBlockIterator(int blockIdx) {
      OnDiskIndex.DataBlock block = (OnDiskIndex.DataBlock)this.dataLevel.getBlock(blockIdx);
      return block.hasCombinedIndex?block.getBlockIndex().iterator(this.keyFetcher):block.getRange(0, block.termCount());
   }

   public Iterator<OnDiskIndex.DataTerm> iteratorAt(ByteBuffer query, OnDiskIndex.IteratorOrder order, boolean inclusive) {
      Expression e = new Expression("", this.comparator);
      Expression.Bound bound = new Expression.Bound(query, inclusive);
      switch(null.$SwitchMap$org$apache$cassandra$index$sasi$disk$OnDiskIndex$IteratorOrder[order.ordinal()]) {
      case 1:
         e.setLower(bound);
         break;
      case 2:
         e.setUpper(bound);
         break;
      default:
         throw new IllegalArgumentException("Unknown order: " + order);
      }

      return new OnDiskIndex.TermIterator(this.levels.length == 0?0:this.getBlockIdx(this.findPointer(query), query), e, order);
   }

   private int getDataBlock(ByteBuffer query) {
      return this.levels.length == 0?0:this.getBlockIdx(this.findPointer(query), query);
   }

   public Iterator<OnDiskIndex.DataTerm> iterator() {
      return new OnDiskIndex.TermIterator(0, new Expression("", this.comparator), OnDiskIndex.IteratorOrder.DESC);
   }

   public void close() throws IOException {
      FileUtils.closeQuietly((Closeable)this.indexFile);
   }

   private OnDiskIndex.PointerTerm findPointer(ByteBuffer query) {
      OnDiskIndex.PointerTerm ptr = null;
      OnDiskIndex.PointerLevel[] var3 = this.levels;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         OnDiskIndex.PointerLevel level = var3[var5];
         if((ptr = level.getPointer(ptr, query)) == null) {
            return null;
         }
      }

      return ptr;
   }

   private OnDiskIndex.DataTerm getTerm(ByteBuffer query) {
      OnDiskBlock.SearchResult<OnDiskIndex.DataTerm> term = this.searchIndex(query, this.getDataBlock(query));
      return term.cmp == 0?(OnDiskIndex.DataTerm)term.result:null;
   }

   private OnDiskBlock.SearchResult<OnDiskIndex.DataTerm> searchIndex(ByteBuffer query, int blockIdx) {
      return ((OnDiskIndex.DataBlock)this.dataLevel.getBlock(blockIdx)).search(this.comparator, query);
   }

   private int getBlockIdx(OnDiskIndex.PointerTerm ptr, ByteBuffer query) {
      int blockIdx = 0;
      if(ptr != null) {
         int cmp = ptr.compareTo(this.comparator, query);
         blockIdx = cmp != 0 && cmp <= 0?ptr.getBlock() + 1:ptr.getBlock();
      }

      return blockIdx;
   }

   public AbstractType<?> getComparator() {
      return this.comparator;
   }

   public String getIndexPath() {
      return this.indexPath;
   }

   private class TermIterator extends AbstractIterator<OnDiskIndex.DataTerm> {
      private final Expression e;
      private final OnDiskIndex.IteratorOrder order;
      protected OnDiskBlock<OnDiskIndex.DataTerm> currentBlock;
      protected int blockIndex;
      protected int offset;
      private boolean checkLower = true;
      private boolean checkUpper = true;

      public TermIterator(int startBlock, Expression expression, OnDiskIndex.IteratorOrder order) {
         this.e = expression;
         this.order = order;
         this.blockIndex = startBlock;
         this.nextBlock();
      }

      protected OnDiskIndex.DataTerm computeNext() {
         while(this.currentBlock != null) {
            if(this.offset >= 0 && this.offset < this.currentBlock.termCount()) {
               OnDiskIndex.DataTerm currentTerm = (OnDiskIndex.DataTerm)this.currentBlock.getTerm(this.nextOffset());
               if((this.e.getOp() != Expression.Op.PREFIX || !currentTerm.isPartial()) && (!this.checkLower || this.e.isLowerSatisfiedBy(currentTerm))) {
                  this.checkLower = false;
                  if(this.checkUpper && !this.e.isUpperSatisfiedBy(currentTerm)) {
                     return (OnDiskIndex.DataTerm)this.endOfData();
                  }

                  return currentTerm;
               }
            } else {
               this.nextBlock();
            }
         }

         return (OnDiskIndex.DataTerm)this.endOfData();
      }

      protected void nextBlock() {
         this.currentBlock = null;
         if(this.blockIndex >= 0 && this.blockIndex < OnDiskIndex.this.dataLevel.blockCount) {
            this.currentBlock = OnDiskIndex.this.dataLevel.getBlock(this.nextBlockIndex());
            this.offset = this.checkLower?this.order.startAt(this.currentBlock, this.e):this.currentBlock.minOffset(this.order);
            this.checkUpper = this.e.hasUpper() && !this.e.isUpperSatisfiedBy((OnDiskIndex.DataTerm)this.currentBlock.getTerm(this.currentBlock.maxOffset(this.order)));
         }
      }

      protected int nextBlockIndex() {
         int current = this.blockIndex;
         this.blockIndex += this.order.step;
         return current;
      }

      protected int nextOffset() {
         int current = this.offset;
         this.offset += this.order.step;
         return current;
      }
   }

   private static class PrefetchedTokensIterator extends RangeIterator<Long, Token> {
      private final NavigableMap<Long, Token> tokens;
      private PeekingIterator<Token> currentIterator;

      public PrefetchedTokensIterator(NavigableMap<Long, Token> tokens) {
         super((Comparable)tokens.firstKey(), (Comparable)tokens.lastKey(), (long)tokens.size());
         this.tokens = tokens;
         this.currentIterator = Iterators.peekingIterator(tokens.values().iterator());
      }

      protected Token computeNext() {
         return this.currentIterator != null && this.currentIterator.hasNext()?(Token)this.currentIterator.next():(Token)this.endOfData();
      }

      protected void performSkipTo(Long nextToken) {
         this.currentIterator = Iterators.peekingIterator(this.tokens.tailMap(nextToken, true).values().iterator());
      }

      public void close() throws IOException {
         this.endOfData();
      }
   }

   protected static class PointerTerm extends Term {
      public PointerTerm(MappedBuffer content, OnDiskIndexBuilder.TermSize size, boolean hasMarkedPartials) {
         super(content, size, hasMarkedPartials);
      }

      public int getBlock() {
         return this.content.getInt(this.getDataOffset());
      }
   }

   public class DataTerm extends Term implements Comparable<OnDiskIndex.DataTerm> {
      private final TokenTree perBlockIndex;

      protected DataTerm(MappedBuffer content, OnDiskIndexBuilder.TermSize size, TokenTree perBlockIndex) {
         super(content, size, OnDiskIndex.this.hasMarkedPartials);
         this.perBlockIndex = perBlockIndex;
      }

      public RangeIterator<Long, Token> getTokens() {
         long blockEnd = FBUtilities.align(this.content.position(), 4096);
         if(this.isSparse()) {
            return new OnDiskIndex.PrefetchedTokensIterator(this.getSparseTokens());
         } else {
            long offset = blockEnd + 4L + (long)this.content.getInt(this.getDataOffset() + 1L);
            return (new TokenTree(OnDiskIndex.this.descriptor, OnDiskIndex.this.indexFile.duplicate().position(offset))).iterator(OnDiskIndex.this.keyFetcher);
         }
      }

      public boolean isSparse() {
         return this.content.get(this.getDataOffset()) > 0;
      }

      public NavigableMap<Long, Token> getSparseTokens() {
         long ptrOffset = this.getDataOffset();
         byte size = this.content.get(ptrOffset);

         assert size > 0;

         NavigableMap<Long, Token> individualTokens = new TreeMap();

         for(int i = 0; i < size; ++i) {
            Token token = this.perBlockIndex.get(this.content.getLong(ptrOffset + 1L + (long)(8 * i)), OnDiskIndex.this.keyFetcher);

            assert token != null;

            individualTokens.put(token.get(), token);
         }

         return individualTokens;
      }

      public int compareTo(OnDiskIndex.DataTerm other) {
         return other == null?1:this.compareTo(OnDiskIndex.this.comparator, other.getTerm());
      }
   }

   protected class PointerBlock extends OnDiskBlock<OnDiskIndex.PointerTerm> {
      public PointerBlock(MappedBuffer block) {
         super(OnDiskIndex.this.descriptor, block, OnDiskBlock.BlockType.POINTER);
      }

      protected OnDiskIndex.PointerTerm cast(MappedBuffer data) {
         return new OnDiskIndex.PointerTerm(data, OnDiskIndex.this.termSize, OnDiskIndex.this.hasMarkedPartials);
      }
   }

   protected class DataBlock extends OnDiskBlock<OnDiskIndex.DataTerm> {
      public DataBlock(MappedBuffer data) {
         super(OnDiskIndex.this.descriptor, data, OnDiskBlock.BlockType.DATA);
      }

      protected OnDiskIndex.DataTerm cast(MappedBuffer data) {
         return OnDiskIndex.this.new DataTerm(data, OnDiskIndex.this.termSize, this.getBlockIndex());
      }

      public RangeIterator<Long, Token> getRange(int start, int end) {
         RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
         NavigableMap<Long, Token> sparse = new TreeMap();

         for(int i = start; i < end; ++i) {
            OnDiskIndex.DataTerm term = (OnDiskIndex.DataTerm)this.getTerm(i);
            if(term.isSparse()) {
               NavigableMap<Long, Token> tokens = term.getSparseTokens();
               Iterator var8 = tokens.entrySet().iterator();

               while(var8.hasNext()) {
                  Entry<Long, Token> t = (Entry)var8.next();
                  Token token = (Token)sparse.get(t.getKey());
                  if(token == null) {
                     sparse.put(t.getKey(), t.getValue());
                  } else {
                     token.merge((CombinedValue)t.getValue());
                  }
               }
            } else {
               builder.add(term.getTokens());
            }
         }

         OnDiskIndex.PrefetchedTokensIterator prefetched = sparse.isEmpty()?null:new OnDiskIndex.PrefetchedTokensIterator(sparse);
         if(builder.rangeCount() == 0) {
            return prefetched;
         } else {
            builder.add(prefetched);
            return builder.build();
         }
      }
   }

   protected abstract class Level<T extends OnDiskBlock> {
      protected final long blockOffsets;
      protected final int blockCount;

      public Level(long offsets, int count) {
         this.blockOffsets = offsets;
         this.blockCount = count;
      }

      public T getBlock(int idx) throws FSReadError {
         assert idx >= 0 && idx < this.blockCount;

         long blockOffset = OnDiskIndex.this.indexFile.getLong(this.blockOffsets + (long)(idx * 8));
         return this.cast(OnDiskIndex.this.indexFile.duplicate().position(blockOffset));
      }

      protected abstract T cast(MappedBuffer var1);
   }

   protected class OnDiskSuperBlock {
      private final TokenTree tokenTree;

      public OnDiskSuperBlock(MappedBuffer buffer) {
         this.tokenTree = new TokenTree(OnDiskIndex.this.descriptor, buffer);
      }

      public RangeIterator<Long, Token> iterator() {
         return this.tokenTree.iterator(OnDiskIndex.this.keyFetcher);
      }
   }

   protected class DataLevel extends OnDiskIndex.Level<OnDiskIndex.DataBlock> {
      protected final int superBlockCnt;
      protected final long superBlocksOffset;

      public DataLevel(long offset, int count) {
         super(offset, count);
         long baseOffset = this.blockOffsets + (long)(this.blockCount * 8);
         this.superBlockCnt = OnDiskIndex.this.indexFile.getInt(baseOffset);
         this.superBlocksOffset = baseOffset + 4L;
      }

      protected OnDiskIndex.DataBlock cast(MappedBuffer block) {
         return OnDiskIndex.this.new DataBlock(block);
      }

      public OnDiskIndex.OnDiskSuperBlock getSuperBlock(int idx) {
         assert idx < this.superBlockCnt : String.format("requested index %d is greater than super block count %d", new Object[]{Integer.valueOf(idx), Integer.valueOf(this.superBlockCnt)});

         long blockOffset = OnDiskIndex.this.indexFile.getLong(this.superBlocksOffset + (long)(idx * 8));
         return OnDiskIndex.this.new OnDiskSuperBlock(OnDiskIndex.this.indexFile.duplicate().position(blockOffset));
      }
   }

   protected class PointerLevel extends OnDiskIndex.Level<OnDiskIndex.PointerBlock> {
      public PointerLevel(long offset, int count) {
         super(offset, count);
      }

      public OnDiskIndex.PointerTerm getPointer(OnDiskIndex.PointerTerm parent, ByteBuffer query) {
         return (OnDiskIndex.PointerTerm)((OnDiskIndex.PointerBlock)this.getBlock(OnDiskIndex.this.getBlockIdx(parent, query))).search(OnDiskIndex.this.comparator, query).result;
      }

      protected OnDiskIndex.PointerBlock cast(MappedBuffer block) {
         return OnDiskIndex.this.new PointerBlock(block);
      }
   }

   public static enum IteratorOrder {
      DESC(1),
      ASC(-1);

      public final int step;

      private IteratorOrder(int step) {
         this.step = step;
      }

      public int startAt(OnDiskBlock<OnDiskIndex.DataTerm> block, Expression e) {
         switch(null.$SwitchMap$org$apache$cassandra$index$sasi$disk$OnDiskIndex$IteratorOrder[this.ordinal()]) {
         case 1:
            return e.lower == null?0:this.startAt(block.search(e.validator, e.lower.value), e.lower.inclusive);
         case 2:
            return e.upper == null?block.termCount() - 1:this.startAt(block.search(e.validator, e.upper.value), e.upper.inclusive);
         default:
            throw new IllegalArgumentException("Unknown order: " + this);
         }
      }

      public int startAt(OnDiskBlock.SearchResult<OnDiskIndex.DataTerm> found, boolean inclusive) {
         switch(null.$SwitchMap$org$apache$cassandra$index$sasi$disk$OnDiskIndex$IteratorOrder[this.ordinal()]) {
         case 1:
            if(found.cmp < 0) {
               return found.index + 1;
            }

            return !inclusive && found.cmp == 0?found.index + 1:found.index;
         case 2:
            if(found.cmp < 0) {
               return found.index;
            }

            return !inclusive || found.cmp != 0 && found.cmp >= 0?found.index - 1:found.index;
         default:
            throw new IllegalArgumentException("Unknown order: " + this);
         }
      }
   }
}
