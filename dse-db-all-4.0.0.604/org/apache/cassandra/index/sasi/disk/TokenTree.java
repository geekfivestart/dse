package org.apache.cassandra.index.sasi.disk;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TokenTree {
   private static final int LONG_BYTES = 8;
   private static final int SHORT_BYTES = 2;
   private final Descriptor descriptor;
   private final MappedBuffer file;
   private final long startPos;
   private final long treeMinToken;
   private final long treeMaxToken;
   private final long tokenCount;

   @VisibleForTesting
   protected TokenTree(MappedBuffer tokenTree) {
      this(Descriptor.CURRENT, tokenTree);
   }

   public TokenTree(Descriptor d, MappedBuffer tokenTree) {
      this.descriptor = d;
      this.file = tokenTree;
      this.startPos = this.file.position();
      this.file.position(this.startPos + 19L);
      if(!this.validateMagic()) {
         throw new IllegalArgumentException("invalid token tree");
      } else {
         this.tokenCount = this.file.getLong();
         this.treeMinToken = this.file.getLong();
         this.treeMaxToken = this.file.getLong();
      }
   }

   public long getCount() {
      return this.tokenCount;
   }

   public RangeIterator<Long, Token> iterator(Function<Long, DecoratedKey> keyFetcher) {
      return new TokenTree.TokenTreeIterator(this.file.duplicate(), keyFetcher);
   }

   public TokenTree.OnDiskToken get(long searchToken, Function<Long, DecoratedKey> keyFetcher) {
      this.seekToLeaf(searchToken, this.file);
      long leafStart = this.file.position();
      short leafSize = this.file.getShort(leafStart + 1L);
      this.file.position(leafStart + 64L);
      short tokenIndex = this.searchLeaf(searchToken, leafSize);
      this.file.position(leafStart + 64L);
      TokenTree.OnDiskToken token = TokenTree.OnDiskToken.getTokenAt(this.file, tokenIndex, leafSize, keyFetcher);
      return token.get().equals(Long.valueOf(searchToken))?token:null;
   }

   private boolean validateMagic() {
      String var1 = this.descriptor.version.toString();
      byte var2 = -1;
      switch(var1.hashCode()) {
      case 3104:
         if(var1.equals("aa")) {
            var2 = 0;
         }
         break;
      case 3105:
         if(var1.equals("ab")) {
            var2 = 1;
         }
      }

      switch(var2) {
      case 0:
         return true;
      case 1:
         return 23121 == this.file.getShort();
      default:
         return false;
      }
   }

   private void seekToLeaf(long token, MappedBuffer file) {
      long blockStart = this.startPos;

      while(true) {
         file.position(blockStart);
         byte info = file.get();
         boolean isLeaf = (info & 1) == 1;
         if(isLeaf) {
            file.position(blockStart);
            return;
         }

         short tokenCount = file.getShort();
         long minToken = file.getLong();
         long maxToken = file.getLong();
         long seekBase = blockStart + 64L;
         if(minToken > token) {
            file.position(seekBase + (long)(tokenCount * 8));
            blockStart = this.startPos + (long)((int)file.getLong());
         } else if(maxToken < token) {
            file.position(seekBase + (long)(2 * tokenCount * 8));
            blockStart = this.startPos + (long)((int)file.getLong());
         } else {
            file.position(seekBase);
            short offsetIndex = this.searchBlock(token, tokenCount, file);
            if(offsetIndex == tokenCount) {
               file.position(file.position() + (long)(offsetIndex * 8));
            } else {
               file.position(file.position() + (long)((tokenCount - offsetIndex - 1 + offsetIndex) * 8));
            }

            blockStart = this.startPos + (long)((int)file.getLong());
         }
      }
   }

   private short searchBlock(long searchToken, short tokenCount, MappedBuffer file) {
      short offsetIndex = 0;

      for(int i = 0; i < tokenCount; ++i) {
         long readToken = file.getLong();
         if(searchToken < readToken) {
            break;
         }

         ++offsetIndex;
      }

      return offsetIndex;
   }

   private short searchLeaf(long searchToken, short tokenCount) {
      long base = this.file.position();
      int start = 0;
      int end = tokenCount;
      int middle = 0;

      while(start <= end) {
         middle = start + (end - start >> 1);
         long token = this.file.getLong(base + (long)(middle * 16 + 4));
         if(token == searchToken) {
            break;
         }

         if(token < searchToken) {
            start = middle + 1;
         } else {
            end = middle - 1;
         }
      }

      return (short)middle;
   }

   private static class KeyIterator extends AbstractIterator<DecoratedKey> {
      private final Function<Long, DecoratedKey> keyFetcher;
      private final long[] offsets;
      private int index = 0;

      public KeyIterator(Function<Long, DecoratedKey> keyFetcher, long[] offsets) {
         this.keyFetcher = keyFetcher;
         this.offsets = offsets;
      }

      public DecoratedKey computeNext() {
         return this.index < this.offsets.length?(DecoratedKey)this.keyFetcher.apply(Long.valueOf(this.offsets[this.index++])):(DecoratedKey)this.endOfData();
      }
   }

   private static class TokenInfo {
      private final MappedBuffer buffer;
      private final Function<Long, DecoratedKey> keyFetcher;
      private final long position;
      private final short leafSize;

      public TokenInfo(MappedBuffer buffer, long position, short leafSize, Function<Long, DecoratedKey> keyFetcher) {
         this.keyFetcher = keyFetcher;
         this.buffer = buffer;
         this.position = position;
         this.leafSize = leafSize;
      }

      public Iterator<DecoratedKey> iterator() {
         return new TokenTree.KeyIterator(this.keyFetcher, this.fetchOffsets());
      }

      public int hashCode() {
         return (new HashCodeBuilder()).append(this.keyFetcher).append(this.position).append(this.leafSize).build().intValue();
      }

      public boolean equals(Object other) {
         if(!(other instanceof TokenTree.TokenInfo)) {
            return false;
         } else {
            TokenTree.TokenInfo o = (TokenTree.TokenInfo)other;
            return this.keyFetcher == o.keyFetcher && this.position == o.position;
         }
      }

      private long[] fetchOffsets() {
         short info = this.buffer.getShort(this.position);
         int offsetExtra = this.buffer.getShort(this.position + 2L) & 65535;
         int offsetData = this.buffer.getInt(this.position + 4L + 8L);
         TokenTreeBuilder.EntryType type = TokenTreeBuilder.EntryType.of(info & 3);
         switch (type) {
            case SIMPLE: {
               return new long[]{offsetData};
            }
            case OVERFLOW: {
               long[] offsets = new long[offsetExtra];
               long offsetPos = this.buffer.position() + (long)(2 * (this.leafSize * 8)) + (long)(offsetData * 8);
               for (int i = 0; i < offsetExtra; ++i) {
                  offsets[i] = this.buffer.getLong(offsetPos + (long)(i * 8));
               }
               return offsets;
            }
            case FACTORED: {
               return new long[]{((long)offsetData << 16) + (long)offsetExtra};
            }
            case PACKED: {
               return new long[]{offsetExtra, offsetData};
            }
         }
         throw new IllegalStateException("Unknown entry type: " + (Object)((Object)type));
      }
   }

   public static class OnDiskToken extends Token {
      private final Set<TokenTree.TokenInfo> info = new HashSet(2);
      private final Set<DecoratedKey> loadedKeys;

      public OnDiskToken(MappedBuffer buffer, long position, short leafSize, Function<Long, DecoratedKey> keyFetcher) {
         super(buffer.getLong(position + 4L));
         this.loadedKeys = new TreeSet(DecoratedKey.comparator);
         this.info.add(new TokenTree.TokenInfo(buffer, position, leafSize, keyFetcher));
      }

      public void merge(CombinedValue<Long> other) {
         if(other instanceof Token) {
            Token o = (Token)other;
            if(this.token != o.token) {
               throw new IllegalArgumentException(String.format("%s != %s", new Object[]{Long.valueOf(this.token), Long.valueOf(o.token)}));
            } else {
               if(o instanceof TokenTree.OnDiskToken) {
                  this.info.addAll(((TokenTree.OnDiskToken)other).info);
               } else {
                  Iterators.addAll(this.loadedKeys, o.iterator());
               }

            }
         }
      }

      public Iterator<DecoratedKey> iterator() {
         List<Iterator<DecoratedKey>> keys = new ArrayList(this.info.size());
         Iterator var2 = this.info.iterator();

         while(var2.hasNext()) {
            TokenTree.TokenInfo i = (TokenTree.TokenInfo)var2.next();
            keys.add(i.iterator());
         }

         if(!this.loadedKeys.isEmpty()) {
            keys.add(this.loadedKeys.iterator());
         }

         return MergeIterator.get(keys, DecoratedKey.comparator, new Reducer<DecoratedKey, DecoratedKey>() {
            DecoratedKey reduced = null;

            public boolean trivialReduceIsTrivial() {
               return true;
            }

            public void reduce(int idx, DecoratedKey current) {
               this.reduced = current;
            }

            public DecoratedKey getReduced() {
               return this.reduced;
            }
         });
      }

      public LongSet getOffsets() {
         LongSet offsets = new LongHashSet(4);
         Iterator var2 = this.info.iterator();

         while(var2.hasNext()) {
            TokenTree.TokenInfo i = (TokenTree.TokenInfo)var2.next();
            long[] var4 = i.fetchOffsets();
            int var5 = var4.length;

            for(int var6 = 0; var6 < var5; ++var6) {
               long offset = var4[var6];
               offsets.add(offset);
            }
         }

         return offsets;
      }

      public static TokenTree.OnDiskToken getTokenAt(MappedBuffer buffer, int idx, short leafSize, Function<Long, DecoratedKey> keyFetcher) {
         return new TokenTree.OnDiskToken(buffer, getEntryPosition(idx, buffer), leafSize, keyFetcher);
      }

      private static long getEntryPosition(int idx, MappedBuffer file) {
         return file.position() + (long)(idx * 16);
      }
   }

   public class TokenTreeIterator extends RangeIterator<Long, Token> {
      private final Function<Long, DecoratedKey> keyFetcher;
      private final MappedBuffer file;
      private long currentLeafStart;
      private int currentTokenIndex;
      private long leafMinToken;
      private long leafMaxToken;
      private short leafSize;
      protected boolean firstIteration = true;
      private boolean lastLeaf;

      TokenTreeIterator(MappedBuffer file, Function<Long, DecoratedKey> keyFetcher) {
         super(Long.valueOf(TokenTree.this.treeMinToken), Long.valueOf(TokenTree.this.treeMaxToken), TokenTree.this.tokenCount);
         this.file = file;
         this.keyFetcher = keyFetcher;
      }

      protected Token computeNext() {
         this.maybeFirstIteration();
         if(this.currentTokenIndex >= this.leafSize && this.lastLeaf) {
            return (Token)this.endOfData();
         } else if(this.currentTokenIndex < this.leafSize) {
            return this.getTokenAt(this.currentTokenIndex++);
         } else {
            assert !this.lastLeaf;

            this.seekToNextLeaf();
            this.setupBlock();
            return this.computeNext();
         }
      }

      protected void performSkipTo(Long nextToken) {
         this.maybeFirstIteration();
         if(nextToken.longValue() <= this.leafMaxToken) {
            this.searchLeaf(nextToken.longValue());
         } else {
            TokenTree.this.seekToLeaf(nextToken.longValue(), this.file);
            this.setupBlock();
            this.findNearest(nextToken);
         }

      }

      private void setupBlock() {
         this.currentLeafStart = this.file.position();
         this.currentTokenIndex = 0;
         this.lastLeaf = (this.file.get() & 2) > 0;
         this.leafSize = this.file.getShort();
         this.leafMinToken = this.file.getLong();
         this.leafMaxToken = this.file.getLong();
         this.file.position(this.currentLeafStart + 64L);
      }

      private void findNearest(Long next) {
         if(next.longValue() > this.leafMaxToken && !this.lastLeaf) {
            this.seekToNextLeaf();
            this.setupBlock();
            this.findNearest(next);
         } else if(next.longValue() > this.leafMinToken) {
            this.searchLeaf(next.longValue());
         }

      }

      private void searchLeaf(long next) {
         for(int i = this.currentTokenIndex; i < this.leafSize && this.compareTokenAt(this.currentTokenIndex, next) < 0; ++i) {
            ++this.currentTokenIndex;
         }

      }

      private int compareTokenAt(int idx, long toToken) {
         return Long.compare(this.file.getLong(this.getTokenPosition(idx)), toToken);
      }

      private Token getTokenAt(int idx) {
         return TokenTree.OnDiskToken.getTokenAt(this.file, idx, this.leafSize, this.keyFetcher);
      }

      private long getTokenPosition(int idx) {
         return TokenTree.OnDiskToken.getEntryPosition(idx, this.file) + 4L;
      }

      private void seekToNextLeaf() {
         this.file.position(this.currentLeafStart + 4096L);
      }

      public void close() throws IOException {
      }

      private void maybeFirstIteration() {
         if(this.firstIteration) {
            TokenTree.this.seekToLeaf(TokenTree.this.treeMinToken, this.file);
            this.setupBlock();
            this.firstIteration = false;
         }
      }
   }
}
