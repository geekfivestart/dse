package org.apache.cassandra.index.sasi.disk;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.ShortArrayList;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.sa.IndexedTerm;
import org.apache.cassandra.index.sasi.sa.IntegralSA;
import org.apache.cassandra.index.sasi.sa.SA;
import org.apache.cassandra.index.sasi.sa.SuffixSA;
import org.apache.cassandra.index.sasi.sa.TermIterator;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnDiskIndexBuilder {
   private static final Logger logger = LoggerFactory.getLogger(OnDiskIndexBuilder.class);
   public static final int BLOCK_SIZE = 4096;
   public static final int MAX_TERM_SIZE = 1024;
   public static final int SUPER_BLOCK_SIZE = 64;
   public static final int IS_PARTIAL_BIT = 15;
   private static final SequentialWriterOption WRITER_OPTION;
   private final List<OnDiskIndexBuilder.MutableLevel<OnDiskIndexBuilder.InMemoryPointerTerm>> levels;
   private OnDiskIndexBuilder.MutableLevel<OnDiskIndexBuilder.InMemoryDataTerm> dataLevel;
   private final OnDiskIndexBuilder.TermSize termSize;
   private final AbstractType<?> keyComparator;
   private final AbstractType<?> termComparator;
   private final Map<ByteBuffer, TokenTreeBuilder> terms;
   private final OnDiskIndexBuilder.Mode mode;
   private final boolean marksPartials;
   private ByteBuffer minKey;
   private ByteBuffer maxKey;
   private long estimatedBytes;

   public OnDiskIndexBuilder(AbstractType<?> keyComparator, AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode) {
      this(keyComparator, comparator, mode, true);
   }

   public OnDiskIndexBuilder(AbstractType<?> keyComparator, AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode, boolean marksPartials) {
      this.levels = new ArrayList();
      this.keyComparator = keyComparator;
      this.termComparator = comparator;
      this.terms = new HashMap();
      this.termSize = OnDiskIndexBuilder.TermSize.sizeOf(comparator);
      this.mode = mode;
      this.marksPartials = marksPartials;
   }

   public OnDiskIndexBuilder add(ByteBuffer term, DecoratedKey key, long keyPosition) {
      if(term.remaining() >= 1024) {
         logger.error("Rejecting value (value size {}, maximum size {}).", FBUtilities.prettyPrintMemory((long)term.remaining()), FBUtilities.prettyPrintMemory(32767L));
         return this;
      } else {
         TokenTreeBuilder tokens = (TokenTreeBuilder)this.terms.get(term);
         if(tokens == null) {
            this.terms.put(term, tokens = new DynamicTokenTreeBuilder());
            this.estimatedBytes += (long)(112 + term.remaining());
         }

         ((TokenTreeBuilder)tokens).add((Long)key.getToken().getTokenValue(), keyPosition);
         this.minKey = this.minKey != null && this.keyComparator.compare(this.minKey, key.getKey()) <= 0?this.minKey:key.getKey();
         this.maxKey = this.maxKey != null && this.keyComparator.compare(this.maxKey, key.getKey()) >= 0?this.maxKey:key.getKey();
         this.estimatedBytes += 108L;
         return this;
      }
   }

   public long estimatedMemoryUse() {
      return this.estimatedBytes;
   }

   private void addTerm(OnDiskIndexBuilder.InMemoryDataTerm term, SequentialWriter out) throws IOException {
      OnDiskIndexBuilder.InMemoryPointerTerm ptr = this.dataLevel.add(term);
      if(ptr != null) {
         int var4 = 0;

         OnDiskIndexBuilder.MutableLevel level;
         do {
            level = this.getIndexLevel(var4++, out);
         } while((ptr = level.add(ptr)) != null);

      }
   }

   public boolean isEmpty() {
      return this.terms.isEmpty();
   }

   public void finish(Pair<ByteBuffer, ByteBuffer> range, File file, TermIterator terms) {
      this.finish(Descriptor.CURRENT, range, file, terms);
   }

   public boolean finish(File indexFile) throws FSWriteError {
      return this.finish(Descriptor.CURRENT, indexFile);
   }

   @VisibleForTesting
   protected boolean finish(Descriptor descriptor, File file) throws FSWriteError {
      if(this.terms.isEmpty()) {
         try {
            file.createNewFile();
            return false;
         } catch (IOException var6) {
            throw new FSWriteError(var6, file);
         }
      } else {
         SA sa = (this.termComparator instanceof UTF8Type || this.termComparator instanceof AsciiType) && this.mode == OnDiskIndexBuilder.Mode.CONTAINS?new SuffixSA(this.termComparator, this.mode):new IntegralSA(this.termComparator, this.mode);
         Iterator var4 = this.terms.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<ByteBuffer, TokenTreeBuilder> term = (Entry)var4.next();
            ((SA)sa).add((ByteBuffer)term.getKey(), (TokenTreeBuilder)term.getValue());
         }

         this.finish(descriptor, Pair.create(this.minKey, this.maxKey), file, ((SA)sa).finish());
         return true;
      }
   }

   protected void finish(Descriptor descriptor, Pair<ByteBuffer, ByteBuffer> range, File file, TermIterator terms) {
      SequentialWriter out = null;

      try {
         out = new SequentialWriter(file, WRITER_OPTION);
         out.writeUTF(descriptor.version.toString());
         out.writeShort(this.termSize.size);
         ByteBufferUtil.writeWithShortLength((ByteBuffer)terms.minTerm(), (DataOutputPlus)out);
         ByteBufferUtil.writeWithShortLength((ByteBuffer)terms.maxTerm(), (DataOutputPlus)out);
         ByteBufferUtil.writeWithShortLength((ByteBuffer)((ByteBuffer)range.left), (DataOutputPlus)out);
         ByteBufferUtil.writeWithShortLength((ByteBuffer)((ByteBuffer)range.right), (DataOutputPlus)out);
         out.writeUTF(this.mode.toString());
         out.writeBoolean(this.marksPartials);
         out.skipBytes((int)(4096L - out.position()));
         this.dataLevel = (OnDiskIndexBuilder.MutableLevel)(this.mode == OnDiskIndexBuilder.Mode.SPARSE?new OnDiskIndexBuilder.DataBuilderLevel(out, new OnDiskIndexBuilder.MutableDataBlock(this.termComparator, this.mode)):new OnDiskIndexBuilder.MutableLevel(out, new OnDiskIndexBuilder.MutableDataBlock(this.termComparator, this.mode)));

         while(terms.hasNext()) {
            Pair<IndexedTerm, TokenTreeBuilder> term = (Pair)terms.next();
            this.addTerm(new OnDiskIndexBuilder.InMemoryDataTerm((IndexedTerm)term.left, (TokenTreeBuilder)term.right), out);
         }

         this.dataLevel.finalFlush();
         Iterator var14 = this.levels.iterator();

         while(var14.hasNext()) {
            OnDiskIndexBuilder.MutableLevel l = (OnDiskIndexBuilder.MutableLevel)var14.next();
            l.flush();
         }

         long levelIndexPosition = out.position();
         out.writeInt(this.levels.size());

         for(int i = this.levels.size() - 1; i >= 0; --i) {
            ((OnDiskIndexBuilder.MutableLevel)this.levels.get(i)).flushMetadata();
         }

         this.dataLevel.flushMetadata();
         out.writeLong(levelIndexPosition);
         out.sync();
      } catch (IOException var12) {
         throw new FSWriteError(var12, file);
      } finally {
         FileUtils.closeQuietly((Closeable)out);
      }
   }

   private OnDiskIndexBuilder.MutableLevel<OnDiskIndexBuilder.InMemoryPointerTerm> getIndexLevel(int idx, SequentialWriter out) {
      if(this.levels.size() == 0) {
         this.levels.add(new OnDiskIndexBuilder.MutableLevel(out, new OnDiskIndexBuilder.MutableBlock()));
      }

      if(this.levels.size() - 1 < idx) {
         int toAdd = idx - (this.levels.size() - 1);

         for(int i = 0; i < toAdd; ++i) {
            this.levels.add(new OnDiskIndexBuilder.MutableLevel(out, new OnDiskIndexBuilder.MutableBlock()));
         }
      }

      return (OnDiskIndexBuilder.MutableLevel)this.levels.get(idx);
   }

   protected static void alignToBlock(SequentialWriter out) throws IOException {
      long endOfBlock = out.position();
      if((endOfBlock & 4095L) != 0L) {
         out.skipBytes((int)(FBUtilities.align(endOfBlock, 4096) - endOfBlock));
      }

   }

   static {
      WRITER_OPTION = SequentialWriterOption.newBuilder().bufferSize(4096).bufferType(BufferType.OFF_HEAP).build();
   }

   private static class MutableDataBlock extends OnDiskIndexBuilder.MutableBlock<OnDiskIndexBuilder.InMemoryDataTerm> {
      private static final int MAX_KEYS_SPARSE = 5;
      private final AbstractType<?> comparator;
      private final OnDiskIndexBuilder.Mode mode;
      private int offset = 0;
      private final List<TokenTreeBuilder> containers = new ArrayList();
      private TokenTreeBuilder combinedIndex;

      public MutableDataBlock(AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode) {
         this.comparator = comparator;
         this.mode = mode;
         this.combinedIndex = this.initCombinedIndex();
      }

      protected void addInternal(OnDiskIndexBuilder.InMemoryDataTerm term) throws IOException {
         TokenTreeBuilder keys = term.keys;
         if(this.mode == OnDiskIndexBuilder.Mode.SPARSE) {
            if(keys.getTokenCount() > 5L) {
               throw new IOException(String.format("Term - '%s' belongs to more than %d keys in %s mode, which is not allowed.", new Object[]{this.comparator.getString(term.term.getBytes()), Integer.valueOf(5), this.mode.name()}));
            }

            this.writeTerm(term, keys);
         } else {
            this.writeTerm(term, this.offset);
            this.offset += keys.serializedSize();
            this.containers.add(keys);
         }

         if(this.mode == OnDiskIndexBuilder.Mode.SPARSE) {
            this.combinedIndex.add(keys);
         }

      }

      protected int sizeAfter(OnDiskIndexBuilder.InMemoryDataTerm element) {
         return super.sizeAfter(element) + this.ptrLength(element);
      }

      public void flushAndClear(SequentialWriter out) throws IOException {
         super.flushAndClear(out);
         out.writeInt(this.mode == OnDiskIndexBuilder.Mode.SPARSE?this.offset:-1);
         if(this.containers.size() > 0) {
            Iterator var2 = this.containers.iterator();

            while(var2.hasNext()) {
               TokenTreeBuilder tokens = (TokenTreeBuilder)var2.next();
               tokens.write(out);
            }
         }

         if(this.mode == OnDiskIndexBuilder.Mode.SPARSE && this.combinedIndex != null) {
            this.combinedIndex.finish().write(out);
         }

         OnDiskIndexBuilder.alignToBlock(out);
         this.containers.clear();
         this.combinedIndex = this.initCombinedIndex();
         this.offset = 0;
      }

      private int ptrLength(OnDiskIndexBuilder.InMemoryDataTerm term) {
         return term.keys.getTokenCount() > 5L?5:1 + 8 * (int)term.keys.getTokenCount();
      }

      private void writeTerm(OnDiskIndexBuilder.InMemoryTerm term, TokenTreeBuilder keys) throws IOException {
         term.serialize(this.buffer);
         this.buffer.writeByte((byte)((int)keys.getTokenCount()));
         Iterator var3 = keys.iterator();

         while(var3.hasNext()) {
            Pair<Long, LongSet> key = (Pair)var3.next();
            this.buffer.writeLong(((Long)key.left).longValue());
         }

      }

      private void writeTerm(OnDiskIndexBuilder.InMemoryTerm term, int offset) throws IOException {
         term.serialize(this.buffer);
         this.buffer.writeByte(0);
         this.buffer.writeInt(offset);
      }

      private TokenTreeBuilder initCombinedIndex() {
         return this.mode == OnDiskIndexBuilder.Mode.SPARSE?new DynamicTokenTreeBuilder():null;
      }
   }

   private static class MutableBlock<T extends OnDiskIndexBuilder.InMemoryTerm> {
      protected final DataOutputBufferFixed buffer = new DataOutputBufferFixed(4096);
      protected final ShortArrayList offsets = new ShortArrayList();

      public MutableBlock() {
      }

      public final void add(T term) throws IOException {
         this.offsets.add((short)((int)this.buffer.position()));
         this.addInternal(term);
      }

      protected void addInternal(T term) throws IOException {
         term.serialize(this.buffer);
      }

      public boolean hasSpaceFor(T element) {
         return this.sizeAfter(element) < 4096;
      }

      protected int sizeAfter(T element) {
         return this.getWatermark() + 4 + element.serializedSize();
      }

      protected int getWatermark() {
         return 4 + this.offsets.size() * 2 + (int)this.buffer.position();
      }

      public void flushAndClear(SequentialWriter out) throws IOException {
         out.writeInt(this.offsets.size());

         for(int i = 0; i < this.offsets.size(); ++i) {
            out.writeShort(this.offsets.get(i));
         }

         out.write(this.buffer.buffer());
         OnDiskIndexBuilder.alignToBlock(out);
         this.offsets.clear();
         this.buffer.clear();
      }
   }

   private class DataBuilderLevel extends OnDiskIndexBuilder.MutableLevel<OnDiskIndexBuilder.InMemoryDataTerm> {
      private final LongArrayList superBlockOffsets = new LongArrayList();
      private int dataBlocksCnt;
      private TokenTreeBuilder superBlockTree = new DynamicTokenTreeBuilder();

      public DataBuilderLevel(SequentialWriter out, OnDiskIndexBuilder.MutableBlock<OnDiskIndexBuilder.InMemoryDataTerm> block) {
         super(out, block);
      }

      public OnDiskIndexBuilder.InMemoryPointerTerm add(OnDiskIndexBuilder.InMemoryDataTerm term) throws IOException {
         OnDiskIndexBuilder.InMemoryPointerTerm ptr = super.add(term);
         if(ptr != null) {
            ++this.dataBlocksCnt;
            this.flushSuperBlock(false);
         }

         this.superBlockTree.add(term.keys);
         return ptr;
      }

      public void flushSuperBlock(boolean force) throws IOException {
         if(this.dataBlocksCnt == 64 || force && !this.superBlockTree.isEmpty()) {
            this.superBlockOffsets.add(this.out.position());
            this.superBlockTree.finish().write(this.out);
            OnDiskIndexBuilder.alignToBlock(this.out);
            this.dataBlocksCnt = 0;
            this.superBlockTree = new DynamicTokenTreeBuilder();
         }

      }

      public void finalFlush() throws IOException {
         super.flush();
         this.flushSuperBlock(true);
      }

      public void flushMetadata() throws IOException {
         super.flushMetadata();
         this.flushMetadata(this.superBlockOffsets);
      }
   }

   private class MutableLevel<T extends OnDiskIndexBuilder.InMemoryTerm> {
      private final LongArrayList blockOffsets = new LongArrayList();
      protected final SequentialWriter out;
      private final OnDiskIndexBuilder.MutableBlock<T> inProcessBlock;
      private OnDiskIndexBuilder.InMemoryPointerTerm lastTerm;

      public MutableLevel(SequentialWriter out, OnDiskIndexBuilder.MutableBlock<T> block) {
         this.out = out;
         this.inProcessBlock = block;
      }

      public OnDiskIndexBuilder.InMemoryPointerTerm add(T term) throws IOException {
         OnDiskIndexBuilder.InMemoryPointerTerm toPromote = null;
         if(!this.inProcessBlock.hasSpaceFor(term)) {
            this.flush();
            toPromote = this.lastTerm;
         }

         this.inProcessBlock.add(term);
         this.lastTerm = OnDiskIndexBuilder.this.new InMemoryPointerTerm(term.term, this.blockOffsets.size());
         return toPromote;
      }

      public void flush() throws IOException {
         this.blockOffsets.add(this.out.position());
         this.inProcessBlock.flushAndClear(this.out);
      }

      public void finalFlush() throws IOException {
         this.flush();
      }

      public void flushMetadata() throws IOException {
         this.flushMetadata(this.blockOffsets);
      }

      protected void flushMetadata(LongArrayList longArrayList) throws IOException {
         this.out.writeInt(longArrayList.size());

         for(int i = 0; i < longArrayList.size(); ++i) {
            this.out.writeLong(longArrayList.get(i));
         }

      }
   }

   private class InMemoryDataTerm extends OnDiskIndexBuilder.InMemoryTerm {
      private final TokenTreeBuilder keys;

      public InMemoryDataTerm(IndexedTerm term, TokenTreeBuilder keys) {
         super(term);
         this.keys = keys;
      }
   }

   private class InMemoryPointerTerm extends OnDiskIndexBuilder.InMemoryTerm {
      protected final int blockCnt;

      public InMemoryPointerTerm(IndexedTerm term, int blockCnt) {
         super(term);
         this.blockCnt = blockCnt;
      }

      public int serializedSize() {
         return super.serializedSize() + 4;
      }

      public void serialize(DataOutputPlus out) throws IOException {
         super.serialize(out);
         out.writeInt(this.blockCnt);
      }
   }

   private class InMemoryTerm {
      protected final IndexedTerm term;

      public InMemoryTerm(IndexedTerm term) {
         this.term = term;
      }

      public int serializedSize() {
         return (OnDiskIndexBuilder.this.termSize.isConstant()?0:2) + this.term.getBytes().remaining();
      }

      public void serialize(DataOutputPlus out) throws IOException {
         if(OnDiskIndexBuilder.this.termSize.isConstant()) {
            out.write(this.term.getBytes());
         } else {
            out.writeShort(this.term.getBytes().remaining() | (OnDiskIndexBuilder.this.marksPartials && this.term.isPartial()?1:0) << 15);
            out.write(this.term.getBytes());
         }

      }
   }

   public static enum TermSize {
      INT(4),
      LONG(8),
      UUID(16),
      VARIABLE(-1);

      public final int size;

      private TermSize(int size) {
         this.size = size;
      }

      public boolean isConstant() {
         return this != VARIABLE;
      }

      public static OnDiskIndexBuilder.TermSize of(int size) {
         switch(size) {
         case -1:
            return VARIABLE;
         case 4:
            return INT;
         case 8:
            return LONG;
         case 16:
            return UUID;
         default:
            throw new IllegalStateException("unknown state: " + size);
         }
      }

      public static OnDiskIndexBuilder.TermSize sizeOf(AbstractType<?> comparator) {
         return !(comparator instanceof Int32Type) && !(comparator instanceof FloatType)?(!(comparator instanceof LongType) && !(comparator instanceof DoubleType) && !(comparator instanceof TimestampType) && !(comparator instanceof DateType)?(!(comparator instanceof TimeUUIDType) && !(comparator instanceof UUIDType)?VARIABLE:UUID):LONG):INT;
      }
   }

   public static enum Mode {
      PREFIX(EnumSet.of(Expression.Op.EQ, Expression.Op.MATCH, Expression.Op.PREFIX, Expression.Op.NOT_EQ, Expression.Op.RANGE)),
      CONTAINS(EnumSet.of(Expression.Op.EQ, new Expression.Op[]{Expression.Op.MATCH, Expression.Op.CONTAINS, Expression.Op.PREFIX, Expression.Op.SUFFIX, Expression.Op.NOT_EQ})),
      SPARSE(EnumSet.of(Expression.Op.EQ, Expression.Op.NOT_EQ, Expression.Op.RANGE));

      Set<Expression.Op> supportedOps;

      private Mode(Set<Expression.Op> ops) {
         this.supportedOps = ops;
      }

      public static OnDiskIndexBuilder.Mode mode(String mode) {
         return valueOf(mode.toUpperCase());
      }

      public boolean supports(Expression.Op op) {
         return this.supportedOps.contains(op);
      }
   }
}
