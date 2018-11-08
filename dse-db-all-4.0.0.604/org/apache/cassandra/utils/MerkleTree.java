package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.PeekingIterator;
import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IPartitionerDependentSerializer;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MerkleTree implements Serializable {
   public static final Versioned<RepairVerbs.RepairVersion, MerkleTree.MerkleTreeSerializer> serializers = RepairVerbs.RepairVersion.versioned(MerkleTree.MerkleTreeSerializer::<init>);
   private static Logger logger = LoggerFactory.getLogger(MerkleTree.class);
   private static final long serialVersionUID = 2L;
   public static final byte RECOMMENDED_DEPTH = 126;
   public static final int CONSISTENT = 0;
   public static final int FULLY_INCONSISTENT = 1;
   public static final int PARTIALLY_INCONSISTENT = 2;
   public static final byte[] EMPTY_HASH = new byte[0];
   public final byte hashdepth;
   public final Range<Token> fullRange;
   private final IPartitioner partitioner;
   private long maxsize;
   private long size;
   private MerkleTree.Hashable root;

   public MerkleTree(IPartitioner partitioner, Range<Token> range, byte hashdepth, long maxsize) {
      assert hashdepth < 127;

      this.fullRange = (Range)Objects.requireNonNull(range);
      this.partitioner = (IPartitioner)Objects.requireNonNull(partitioner);
      this.hashdepth = hashdepth;
      this.maxsize = maxsize;
      this.size = 1L;
      this.root = new MerkleTree.Leaf((byte[])null);
   }

   static byte inc(byte in) {
      assert in < 127;

      return (byte)(in + 1);
   }

   public void init() {
      byte sizedepth = (byte)((int)(Math.log10((double)this.maxsize) / Math.log10(2.0D)));
      byte depth = (byte)Math.min(sizedepth, this.hashdepth);
      this.root = this.initHelper((Token)this.fullRange.left, (Token)this.fullRange.right, 0, depth);
      this.size = (long)Math.pow(2.0D, (double)depth);
   }

   private MerkleTree.Hashable initHelper(Token left, Token right, byte depth, byte max) {
      if(depth == max) {
         return new MerkleTree.Leaf();
      } else {
         Token midpoint = this.partitioner.midpoint(left, right);
         if(!midpoint.equals(left) && !midpoint.equals(right)) {
            MerkleTree.Hashable lchild = this.initHelper(left, midpoint, inc(depth), max);
            MerkleTree.Hashable rchild = this.initHelper(midpoint, right, inc(depth), max);
            return new MerkleTree.Inner(midpoint, lchild, rchild);
         } else {
            return new MerkleTree.Leaf();
         }
      }
   }

   @VisibleForTesting
   public MerkleTree.Hashable root() {
      return this.root;
   }

   public IPartitioner partitioner() {
      return this.partitioner;
   }

   public long size() {
      return this.size;
   }

   public long maxsize() {
      return this.maxsize;
   }

   public void maxsize(long maxsize) {
      this.maxsize = maxsize;
   }

   public static List<MerkleTree.TreeRange> difference(MerkleTree ltree, MerkleTree rtree) {
      Stream var10000 = diff(ltree, rtree).stream();
      MerkleTree.TreeRange.class.getClass();
      return (List)var10000.map(MerkleTree.TreeRange.class::cast).collect(Collectors.toList());
   }

   public static List<MerkleTree.TreeDifference> diff(MerkleTree ltree, MerkleTree rtree) {
      if(!ltree.fullRange.equals(rtree.fullRange)) {
         throw new IllegalArgumentException("Difference only make sense on tree covering the same range (but " + ltree.fullRange + " != " + rtree.fullRange + ")");
      } else {
         List<MerkleTree.TreeDifference> diff = new ArrayList();
         MerkleTree.TreeDifference active = new MerkleTree.TreeDifference((Token)ltree.fullRange.left, (Token)ltree.fullRange.right, 0);
         MerkleTree.Hashable lnode = ltree.find(active);
         MerkleTree.Hashable rnode = rtree.find(active);
         byte[] lhash = lnode.hash();
         byte[] rhash = rnode.hash();
         active.setSize(lnode.sizeOfRange(), rnode.sizeOfRange());
         active.setHashes(lhash, rhash);
         if(lhash != null && rhash != null && !Arrays.equals(lhash, rhash)) {
            if(!(lnode instanceof MerkleTree.Leaf) && !(rnode instanceof MerkleTree.Leaf)) {
               logger.trace("Digest mismatch detected, traversing trees [{}, {}]", ltree, rtree);
               if(1 == differenceHelper(ltree, rtree, diff, active)) {
                  logger.trace("Range {} fully inconsistent", active);
                  diff.add(active);
               }
            } else {
               logger.trace("Digest mismatch detected among leaf nodes {}, {}", lnode, rnode);
               diff.add(active);
            }
         } else if(lhash == null || rhash == null) {
            diff.add(active);
         }

         return diff;
      }
   }

   @VisibleForTesting
   static int differenceHelper(MerkleTree ltree, MerkleTree rtree, List<MerkleTree.TreeDifference> diff, MerkleTree.TreeRange active) {
      if(active.depth == 127) {
         return 0;
      } else {
         Token midpoint = ltree.partitioner().midpoint((Token)active.left, (Token)active.right);
         if(!midpoint.equals(active.left) && !midpoint.equals(active.right)) {
            MerkleTree.TreeDifference left = new MerkleTree.TreeDifference((Token)active.left, midpoint, inc(active.depth));
            MerkleTree.TreeDifference right = new MerkleTree.TreeDifference(midpoint, (Token)active.right, inc(active.depth));
            logger.trace("({}) Hashing sub-ranges [{}, {}] for {} divided by midpoint {}", new Object[]{Byte.valueOf(active.depth), left, right, active, midpoint});
            MerkleTree.Hashable lnode = ltree.find(left);
            MerkleTree.Hashable rnode = rtree.find(left);
            byte[] lhash = lnode.hash();
            byte[] rhash = rnode.hash();
            left.setSize(lnode.sizeOfRange(), rnode.sizeOfRange());
            left.setRows(lnode.rowsInRange(), rnode.rowsInRange());
            left.setHashes(lnode.hash, rnode.hash);
            int ldiff = 0;
            boolean lreso = lhash != null && rhash != null;
            if(lreso && !Arrays.equals(lhash, rhash)) {
               logger.trace("({}) Inconsistent digest on left sub-range {}: [{}, {}]", new Object[]{Byte.valueOf(active.depth), left, lnode, rnode});
               if(lnode instanceof MerkleTree.Leaf) {
                  ldiff = 1;
               } else {
                  ldiff = differenceHelper(ltree, rtree, diff, left);
               }
            } else if(!lreso) {
               logger.trace("({}) Left sub-range fully inconsistent {}", Byte.valueOf(active.depth), right);
               ldiff = 1;
            }

            lnode = ltree.find(right);
            rnode = rtree.find(right);
            lhash = lnode.hash();
            rhash = rnode.hash();
            right.setSize(lnode.sizeOfRange(), rnode.sizeOfRange());
            right.setRows(lnode.rowsInRange(), rnode.rowsInRange());
            right.setHashes(lnode.hash, rnode.hash);
            int rdiff = 0;
            boolean rreso = lhash != null && rhash != null;
            if(rreso && !Arrays.equals(lhash, rhash)) {
               logger.trace("({}) Inconsistent digest on right sub-range {}: [{}, {}]", new Object[]{Byte.valueOf(active.depth), right, lnode, rnode});
               if(rnode instanceof MerkleTree.Leaf) {
                  rdiff = 1;
               } else {
                  rdiff = differenceHelper(ltree, rtree, diff, right);
               }
            } else if(!rreso) {
               logger.trace("({}) Right sub-range fully inconsistent {}", Byte.valueOf(active.depth), right);
               rdiff = 1;
            }

            if(ldiff == 1 && rdiff == 1) {
               logger.trace("({}) Fully inconsistent range [{}, {}]", new Object[]{Byte.valueOf(active.depth), left, right});
               return 1;
            } else if(ldiff == 1) {
               logger.trace("({}) Adding left sub-range to diff as fully inconsistent {}", Byte.valueOf(active.depth), left);
               diff.add(left);
               return 2;
            } else if(rdiff == 1) {
               logger.trace("({}) Adding right sub-range to diff as fully inconsistent {}", Byte.valueOf(active.depth), right);
               diff.add(right);
               return 2;
            } else {
               logger.trace("({}) Range {} partially inconstent", Byte.valueOf(active.depth), active);
               return 2;
            }
         } else {
            logger.trace("({}) No sane midpoint ({}) for range {} , marking whole range as inconsistent", new Object[]{Byte.valueOf(active.depth), midpoint, active});
            return 1;
         }
      }
   }

   public MerkleTree.TreeRange get(Token t) {
      return this.getHelper(this.root, (Token)this.fullRange.left, (Token)this.fullRange.right, 0, t);
   }

   MerkleTree.TreeRange getHelper(MerkleTree.Hashable hashable, Token pleft, Token pright, byte depth, Token t) {
      while(!(hashable instanceof MerkleTree.Leaf)) {
         MerkleTree.Inner node = (MerkleTree.Inner)hashable;
         depth = inc(depth);
         if(Range.contains(pleft, node.token, t)) {
            hashable = node.lchild;
            pright = node.token;
         } else {
            hashable = node.rchild;
            pleft = node.token;
         }
      }

      return new MerkleTree.TreeRange(this, pleft, pright, depth, hashable);
   }

   public void invalidate(Token t) {
      this.invalidateHelper(this.root, (Token)this.fullRange.left, t);
   }

   private void invalidateHelper(MerkleTree.Hashable hashable, Token pleft, Token t) {
      hashable.hash((byte[])null);
      if(!(hashable instanceof MerkleTree.Leaf)) {
         MerkleTree.Inner node = (MerkleTree.Inner)hashable;
         if(Range.contains(pleft, node.token, t)) {
            this.invalidateHelper(node.lchild, pleft, t);
         } else {
            this.invalidateHelper(node.rchild, node.token, t);
         }

      }
   }

   public byte[] hash(Range<Token> range) {
      return this.find(range).hash();
   }

   private MerkleTree.Hashable find(Range<Token> range) {
      try {
         return this.findHelper(this.root, new Range(this.fullRange.left, this.fullRange.right), range);
      } catch (MerkleTree.StopRecursion var3) {
         return new MerkleTree.Leaf();
      }
   }

   private MerkleTree.Hashable findHelper(MerkleTree.Hashable current, Range<Token> activeRange, Range<Token> find) throws MerkleTree.StopRecursion {
      while(!(current instanceof MerkleTree.Leaf)) {
         MerkleTree.Inner node = (MerkleTree.Inner)current;
         Range<Token> leftRange = new Range(activeRange.left, node.token);
         Range<Token> rightRange = new Range(node.token, activeRange.right);
         if(find.contains((AbstractBounds)activeRange)) {
            return node.calc();
         }

         if(leftRange.contains((AbstractBounds)find)) {
            current = node.lchild;
            activeRange = leftRange;
         } else {
            if(!rightRange.contains((AbstractBounds)find)) {
               throw new MerkleTree.StopRecursion.BadRange();
            }

            current = node.rchild;
            activeRange = rightRange;
         }
      }

      if(!find.contains((AbstractBounds)activeRange)) {
         throw new MerkleTree.StopRecursion.BadRange();
      } else {
         return current;
      }
   }

   public boolean split(Token t) {
      if(this.size >= this.maxsize) {
         return false;
      } else {
         try {
            this.root = this.splitHelper(this.root, (Token)this.fullRange.left, (Token)this.fullRange.right, 0, t);
            return true;
         } catch (MerkleTree.StopRecursion.TooDeep var3) {
            return false;
         }
      }
   }

   private MerkleTree.Hashable splitHelper(MerkleTree.Hashable hashable, Token pleft, Token pright, byte depth, Token t) throws MerkleTree.StopRecursion.TooDeep {
      if(depth >= this.hashdepth) {
         throw new MerkleTree.StopRecursion.TooDeep();
      } else if(hashable instanceof MerkleTree.Leaf) {
         Token midpoint = this.partitioner.midpoint(pleft, pright);
         if(!midpoint.equals(pleft) && !midpoint.equals(pright)) {
            ++this.size;
            return new MerkleTree.Inner(midpoint, new MerkleTree.Leaf(), new MerkleTree.Leaf());
         } else {
            throw new MerkleTree.StopRecursion.TooDeep();
         }
      } else {
         MerkleTree.Inner node = (MerkleTree.Inner)hashable;
         if(Range.contains(pleft, node.token, t)) {
            node.lchild(this.splitHelper(node.lchild, pleft, node.token, inc(depth), t));
         } else {
            node.rchild(this.splitHelper(node.rchild, node.token, pright, inc(depth), t));
         }

         return node;
      }
   }

   public MerkleTree.TreeRangeIterator invalids() {
      return new MerkleTree.TreeRangeIterator(this);
   }

   public EstimatedHistogram histogramOfRowSizePerLeaf() {
      HistogramBuilder histbuild = new HistogramBuilder();
      Iterator var2 = (new MerkleTree.TreeRangeIterator(this)).iterator();

      while(var2.hasNext()) {
         MerkleTree.TreeRange range = (MerkleTree.TreeRange)var2.next();
         histbuild.add(range.hashable.sizeOfRange);
      }

      return histbuild.buildWithStdevRangesAroundMean();
   }

   public EstimatedHistogram histogramOfRowCountPerLeaf() {
      HistogramBuilder histbuild = new HistogramBuilder();
      Iterator var2 = (new MerkleTree.TreeRangeIterator(this)).iterator();

      while(var2.hasNext()) {
         MerkleTree.TreeRange range = (MerkleTree.TreeRange)var2.next();
         histbuild.add(range.hashable.rowsInRange);
      }

      return histbuild.buildWithStdevRangesAroundMean();
   }

   public long rowCount() {
      long count = 0L;

      MerkleTree.TreeRange range;
      for(Iterator var3 = (new MerkleTree.TreeRangeIterator(this)).iterator(); var3.hasNext(); count += range.hashable.rowsInRange) {
         range = (MerkleTree.TreeRange)var3.next();
      }

      return count;
   }

   public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append("#<MerkleTree root=");
      this.root.toString(buff, 8);
      buff.append(">");
      return buff.toString();
   }

   abstract static class StopRecursion extends Exception {
      StopRecursion() {
      }

      static class TooDeep extends MerkleTree.StopRecursion {
         public TooDeep() {
         }
      }

      static class InvalidHash extends MerkleTree.StopRecursion {
         public InvalidHash() {
         }
      }

      static class BadRange extends MerkleTree.StopRecursion {
         public BadRange() {
         }
      }
   }

   @VisibleForTesting
   public abstract static class Hashable implements Serializable {
      private static final long serialVersionUID = 1L;
      private static final IPartitionerDependentSerializer<MerkleTree.Hashable, RepairVerbs.RepairVersion> serializer = new MerkleTree.Hashable.HashableSerializer();
      protected byte[] hash;
      protected long sizeOfRange;
      protected long rowsInRange;

      protected Hashable(byte[] hash) {
         this.hash = hash;
      }

      public byte[] hash() {
         return this.hash;
      }

      public long sizeOfRange() {
         return this.sizeOfRange;
      }

      public long rowsInRange() {
         return this.rowsInRange;
      }

      void hash(byte[] hash) {
         this.hash = hash;
      }

      MerkleTree.Hashable calc() {
         return this;
      }

      void hash(byte[] lefthash, byte[] righthash) {
         this.hash = binaryHash(lefthash, righthash);
      }

      void addHash(byte[] righthash, long sizeOfRow) {
         if(this.hash == null) {
            this.hash = righthash;
         } else {
            this.hash = binaryHash(this.hash, righthash);
         }

         this.sizeOfRange += sizeOfRow;
         ++this.rowsInRange;
      }

      static byte[] binaryHash(byte[] left, byte[] right) {
         return FBUtilities.xor(left, right);
      }

      public abstract void toString(StringBuilder var1, int var2);

      public static String toString(byte[] hash) {
         return hash == null?"null":"[" + Hex.bytesToHex(hash) + "]";
      }

      private static class HashableSerializer implements IPartitionerDependentSerializer<MerkleTree.Hashable, RepairVerbs.RepairVersion> {
         private HashableSerializer() {
         }

         public void serialize(MerkleTree.Hashable h, DataOutputPlus out, RepairVerbs.RepairVersion version) throws IOException {
            if(h instanceof MerkleTree.Inner) {
               out.writeByte(2);
               MerkleTree.Inner.serializer.serialize((MerkleTree.Inner)h, out, version);
            } else {
               if(!(h instanceof MerkleTree.Leaf)) {
                  throw new IOException("Unexpected Hashable: " + h.getClass().getCanonicalName());
               }

               out.writeByte(1);
               MerkleTree.Leaf.serializer.serialize((MerkleTree.Leaf)h, out, version);
            }

         }

         public MerkleTree.Hashable deserialize(DataInput in, IPartitioner p, RepairVerbs.RepairVersion version) throws IOException {
            byte ident = in.readByte();
            if(2 == ident) {
               return MerkleTree.Inner.serializer.deserialize(in, p, version);
            } else if(1 == ident) {
               return MerkleTree.Leaf.serializer.deserialize(in, p, version);
            } else {
               throw new IOException("Unexpected Hashable: " + ident);
            }
         }

         public int serializedSize(MerkleTree.Hashable h, RepairVerbs.RepairVersion version) {
            if(h instanceof MerkleTree.Inner) {
               return 1 + MerkleTree.Inner.serializer.serializedSize((MerkleTree.Inner)h, version);
            } else if(h instanceof MerkleTree.Leaf) {
               return 1 + MerkleTree.Leaf.serializer.serializedSize((MerkleTree.Leaf)h, version);
            } else {
               throw new AssertionError(h.getClass());
            }
         }
      }
   }

   public static class RowHash {
      public final Token token;
      public final byte[] hash;
      public final long size;

      public RowHash(Token token, byte[] hash, long size) {
         this.token = token;
         this.hash = hash;
         this.size = size;
      }

      public String toString() {
         return "#<RowHash " + this.token + " " + MerkleTree.Hashable.toString(this.hash) + " @ " + this.size + " bytes>";
      }
   }

   static class Leaf extends MerkleTree.Hashable {
      public static final long serialVersionUID = 1L;
      static final byte IDENT = 1;
      private static final MerkleTree.Leaf.LeafSerializer serializer = new MerkleTree.Leaf.LeafSerializer();

      public Leaf() {
         super((byte[])null);
      }

      public Leaf(byte[] hash) {
         super(hash);
      }

      public void toString(StringBuilder buff, int maxdepth) {
         buff.append(this.toString());
      }

      public String toString() {
         return "#<Leaf " + MerkleTree.Hashable.toString(this.hash()) + ">";
      }

      private static class LeafSerializer implements IPartitionerDependentSerializer<MerkleTree.Leaf, RepairVerbs.RepairVersion> {
         private LeafSerializer() {
         }

         public void serialize(MerkleTree.Leaf leaf, DataOutputPlus out, RepairVerbs.RepairVersion version) throws IOException {
            if(leaf.hash == null) {
               out.writeByte(-1);
            } else {
               out.writeByte(leaf.hash.length);
               out.write(leaf.hash);
            }

         }

         public MerkleTree.Leaf deserialize(DataInput in, IPartitioner p, RepairVerbs.RepairVersion version) throws IOException {
            int hashLen = in.readByte();
            byte[] hash = hashLen < 0?null:new byte[hashLen];
            if(hash != null) {
               in.readFully(hash);
            }

            return new MerkleTree.Leaf(hash);
         }

         public int serializedSize(MerkleTree.Leaf leaf, RepairVerbs.RepairVersion version) {
            int size = 1;
            if(leaf.hash != null) {
               size += leaf.hash().length;
            }

            return size;
         }
      }
   }

   static class Inner extends MerkleTree.Hashable {
      public static final long serialVersionUID = 1L;
      static final byte IDENT = 2;
      public final Token token;
      private MerkleTree.Hashable lchild;
      private MerkleTree.Hashable rchild;
      private static final MerkleTree.Inner.InnerSerializer serializer = new MerkleTree.Inner.InnerSerializer();

      public Inner(Token token, MerkleTree.Hashable lchild, MerkleTree.Hashable rchild) {
         super((byte[])null);
         this.token = token;
         this.lchild = lchild;
         this.rchild = rchild;
      }

      public MerkleTree.Hashable lchild() {
         return this.lchild;
      }

      public MerkleTree.Hashable rchild() {
         return this.rchild;
      }

      public void lchild(MerkleTree.Hashable child) {
         this.lchild = child;
      }

      public void rchild(MerkleTree.Hashable child) {
         this.rchild = child;
      }

      MerkleTree.Hashable calc() {
         if(this.hash == null) {
            MerkleTree.Hashable lnode = this.lchild.calc();
            MerkleTree.Hashable rnode = this.rchild.calc();
            this.hash(lnode.hash, rnode.hash);
            this.sizeOfRange = lnode.sizeOfRange + rnode.sizeOfRange;
            this.rowsInRange = lnode.rowsInRange + rnode.rowsInRange;
         }

         return this;
      }

      public void toString(StringBuilder buff, int maxdepth) {
         buff.append("#<").append(this.getClass().getSimpleName());
         buff.append(" ").append(this.token);
         buff.append(" hash=").append(MerkleTree.Hashable.toString(this.hash()));
         buff.append(" children=[");
         if(maxdepth < 1) {
            buff.append("#");
         } else {
            if(this.lchild == null) {
               buff.append("null");
            } else {
               this.lchild.toString(buff, maxdepth - 1);
            }

            buff.append(" ");
            if(this.rchild == null) {
               buff.append("null");
            } else {
               this.rchild.toString(buff, maxdepth - 1);
            }
         }

         buff.append("]>");
      }

      public String toString() {
         StringBuilder buff = new StringBuilder();
         this.toString(buff, 1);
         return buff.toString();
      }

      private static class InnerSerializer implements IPartitionerDependentSerializer<MerkleTree.Inner, RepairVerbs.RepairVersion> {
         private InnerSerializer() {
         }

         public void serialize(MerkleTree.Inner inner, DataOutputPlus out, RepairVerbs.RepairVersion version) throws IOException {
            Token.serializer.serialize(inner.token, out, version.boundsVersion);
            MerkleTree.Hashable.serializer.serialize(inner.lchild, out, version);
            MerkleTree.Hashable.serializer.serialize(inner.rchild, out, version);
         }

         public MerkleTree.Inner deserialize(DataInput in, IPartitioner p, RepairVerbs.RepairVersion version) throws IOException {
            Token token = Token.serializer.deserialize(in, p, version.boundsVersion);
            MerkleTree.Hashable lchild = (MerkleTree.Hashable)MerkleTree.Hashable.serializer.deserialize(in, p, version);
            MerkleTree.Hashable rchild = (MerkleTree.Hashable)MerkleTree.Hashable.serializer.deserialize(in, p, version);
            return new MerkleTree.Inner(token, lchild, rchild);
         }

         public int serializedSize(MerkleTree.Inner inner, RepairVerbs.RepairVersion version) {
            return Token.serializer.serializedSize(inner.token, version.boundsVersion) + MerkleTree.Hashable.serializer.serializedSize(inner.lchild, version) + MerkleTree.Hashable.serializer.serializedSize(inner.rchild, version);
         }
      }
   }

   public static class TreeRangeIterator extends AbstractIterator<MerkleTree.TreeRange> implements Iterable<MerkleTree.TreeRange>, PeekingIterator<MerkleTree.TreeRange> {
      private final ArrayDeque<MerkleTree.TreeRange> tovisit = new ArrayDeque();
      private final MerkleTree tree;

      TreeRangeIterator(MerkleTree tree) {
         this.tovisit.add(new MerkleTree.TreeRange(tree, (Token)tree.fullRange.left, (Token)tree.fullRange.right, 0, tree.root));
         this.tree = tree;
      }

      public MerkleTree.TreeRange computeNext() {
         while(!this.tovisit.isEmpty()) {
            MerkleTree.TreeRange active = (MerkleTree.TreeRange)this.tovisit.pop();
            if(active.hashable instanceof MerkleTree.Leaf) {
               if(active.isWrapAround() && !this.tovisit.isEmpty()) {
                  this.tovisit.addLast(active);
               }

               return active;
            }

            MerkleTree.Inner node = (MerkleTree.Inner)active.hashable;
            MerkleTree.TreeRange left = new MerkleTree.TreeRange(this.tree, (Token)active.left, node.token, MerkleTree.inc(active.depth), node.lchild);
            MerkleTree.TreeRange right = new MerkleTree.TreeRange(this.tree, node.token, (Token)active.right, MerkleTree.inc(active.depth), node.rchild);
            if(right.isWrapAround()) {
               this.tovisit.addLast(left);
               this.tovisit.addFirst(right);
            } else {
               this.tovisit.addFirst(right);
               this.tovisit.addFirst(left);
            }
         }

         return (MerkleTree.TreeRange)this.endOfData();
      }

      public Iterator<MerkleTree.TreeRange> iterator() {
         return this;
      }
   }

   public static class TreeRange extends Range<Token> {
      public static final long serialVersionUID = 1L;
      protected final MerkleTree tree;
      public final byte depth;
      private final MerkleTree.Hashable hashable;

      TreeRange(MerkleTree tree, Token left, Token right, byte depth, MerkleTree.Hashable hashable) {
         super(left, right);
         this.tree = tree;
         this.depth = depth;
         this.hashable = hashable;
      }

      public void hash(byte[] hash) {
         assert this.tree != null : "Not intended for modification!";

         this.hashable.hash(hash);
      }

      public byte[] hash() {
         return this.hashable.hash();
      }

      public void addHash(MerkleTree.RowHash entry) {
         assert this.tree != null : "Not intended for modification!";

         assert this.hashable instanceof MerkleTree.Leaf;

         this.hashable.addHash(entry.hash, entry.size);
      }

      public void ensureHashInitialised() {
         assert this.tree != null : "Not intended for modification!";

         assert this.hashable instanceof MerkleTree.Leaf;

         if(this.hashable.hash == null) {
            this.hashable.hash = MerkleTree.EMPTY_HASH;
         }

      }

      public void addAll(Iterator<MerkleTree.RowHash> entries) {
         while(entries.hasNext()) {
            this.addHash((MerkleTree.RowHash)entries.next());
         }

      }

      public String toString() {
         StringBuilder buff = new StringBuilder("#<TreeRange ");
         buff.append(super.toString()).append(" depth=").append(this.depth);
         return buff.append(">").toString();
      }
   }

   public static class TreeDifference extends MerkleTree.TreeRange {
      private static final long serialVersionUID = 6363654174549968183L;
      private long sizeOnLeft;
      private long sizeOnRight;
      private long rowsOnLeft;
      private long rowsOnRight;
      private byte[] leftHash;
      private byte[] rightHash;

      void setSize(long sizeOnLeft, long sizeOnRight) {
         this.sizeOnLeft = sizeOnLeft;
         this.sizeOnRight = sizeOnRight;
      }

      void setRows(long rowsOnLeft, long rowsOnRight) {
         this.rowsOnLeft = rowsOnLeft;
         this.rowsOnRight = rowsOnRight;
      }

      void setHashes(byte[] leftHash, byte[] rightHash) {
         this.leftHash = leftHash;
         this.rightHash = rightHash;
      }

      public long sizeOnLeft() {
         return this.sizeOnLeft;
      }

      public long sizeOnRight() {
         return this.sizeOnRight;
      }

      public long rowsOnLeft() {
         return this.rowsOnLeft;
      }

      public long rowsOnRight() {
         return this.rowsOnRight;
      }

      public TreeDifference(Token left, Token right, byte depth) {
         super((MerkleTree)null, left, right, depth, (MerkleTree.Hashable)null);
      }

      public long totalRows() {
         return this.rowsOnLeft + this.rowsOnRight;
      }

      public RangeHash getLeftRangeHash() {
         return new RangeHash(this, this.leftHash);
      }

      public RangeHash getRightRangeHash() {
         return new RangeHash(this, this.rightHash);
      }
   }

   public static class MerkleTreeSerializer extends VersionDependent<RepairVerbs.RepairVersion> implements Serializer<MerkleTree> {
      MerkleTreeSerializer(RepairVerbs.RepairVersion version) {
         super(version);
      }

      public void serialize(MerkleTree mt, DataOutputPlus out) throws IOException {
         out.writeByte(mt.hashdepth);
         out.writeLong(mt.maxsize);
         out.writeLong(mt.size);
         out.writeUTF(mt.partitioner.getClass().getCanonicalName());
         Token.serializer.serialize((Token)mt.fullRange.left, out, ((RepairVerbs.RepairVersion)this.version).boundsVersion);
         Token.serializer.serialize((Token)mt.fullRange.right, out, ((RepairVerbs.RepairVersion)this.version).boundsVersion);
         MerkleTree.Hashable.serializer.serialize(mt.root, out, this.version);
      }

      public MerkleTree deserialize(DataInputPlus in) throws IOException {
         byte hashdepth = in.readByte();
         long maxsize = in.readLong();
         long size = in.readLong();

         IPartitioner partitioner;
         try {
            partitioner = FBUtilities.newPartitioner(in.readUTF());
         } catch (ConfigurationException var12) {
            throw new IOException(var12);
         }

         Token left = Token.serializer.deserialize(in, partitioner, (BoundsVersion)((RepairVerbs.RepairVersion)this.version).boundsVersion);
         Token right = Token.serializer.deserialize(in, partitioner, (BoundsVersion)((RepairVerbs.RepairVersion)this.version).boundsVersion);
         Range<Token> fullRange = new Range(left, right);
         MerkleTree mt = new MerkleTree(partitioner, fullRange, hashdepth, maxsize);
         mt.size = size;
         mt.root = (MerkleTree.Hashable)MerkleTree.Hashable.serializer.deserialize(in, partitioner, this.version);
         return mt;
      }

      public long serializedSize(MerkleTree mt) {
         long size = (long)(1 + TypeSizes.sizeof(mt.maxsize) + TypeSizes.sizeof(mt.size) + TypeSizes.sizeof(mt.partitioner.getClass().getCanonicalName()));
         size += (long)Token.serializer.serializedSize((Token)mt.fullRange.left, ((RepairVerbs.RepairVersion)this.version).boundsVersion);
         size += (long)Token.serializer.serializedSize((Token)mt.fullRange.right, ((RepairVerbs.RepairVersion)this.version).boundsVersion);
         size += (long)MerkleTree.Hashable.serializer.serializedSize(mt.root, this.version);
         return size;
      }
   }
}
