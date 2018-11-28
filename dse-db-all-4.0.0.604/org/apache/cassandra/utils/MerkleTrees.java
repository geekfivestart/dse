package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.PeekingIterator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;

public class MerkleTrees implements Iterable<Entry<Range<Token>, MerkleTree>> {
   public static final Versioned<RepairVerbs.RepairVersion, MerkleTrees.MerkleTreesSerializer> serializers = RepairVerbs.RepairVersion.versioned((x$0) -> {
      return new MerkleTrees.MerkleTreesSerializer(x$0);
   });
   private Map<Range<Token>, MerkleTree> merkleTrees;
   private IPartitioner partitioner;

   public MerkleTrees(IPartitioner partitioner) {
      this(partitioner, new ArrayList());
   }

   private MerkleTrees(IPartitioner partitioner, Collection<MerkleTree> merkleTrees) {
      this.merkleTrees = new TreeMap(new MerkleTrees.TokenRangeComparator());
      this.partitioner = partitioner;
      this.addTrees(merkleTrees);
   }

   public Collection<Range<Token>> ranges() {
      return this.merkleTrees.keySet();
   }

   public IPartitioner partitioner() {
      return this.partitioner;
   }

   public void addMerkleTrees(int maxsize, Collection<Range<Token>> ranges) {
      Iterator var3 = ranges.iterator();

      while(var3.hasNext()) {
         Range<Token> range = (Range)var3.next();
         this.addMerkleTree(maxsize, range);
      }

   }

   public MerkleTree addMerkleTree(int maxsize, Range<Token> range) {
      return this.addMerkleTree(maxsize, (byte)126, range);
   }

   @VisibleForTesting
   public MerkleTree addMerkleTree(int maxsize, byte hashdepth, Range<Token> range) {
      MerkleTree tree = new MerkleTree(this.partitioner, range, hashdepth, (long)maxsize);
      this.addTree(tree);
      return tree;
   }

   @VisibleForTesting
   public MerkleTree.TreeRange get(Token t) {
      return this.getMerkleTree(t).get(t);
   }

   public void init() {
      Iterator var1 = this.merkleTrees.keySet().iterator();

      while(var1.hasNext()) {
         Range<Token> range = (Range)var1.next();
         this.init(range);
      }

   }

   public void init(Range<Token> range) {
      ((MerkleTree)this.merkleTrees.get(range)).init();
   }

   public boolean split(Token t) {
      return this.getMerkleTree(t).split(t);
   }

   @VisibleForTesting
   public void invalidate(Token t) {
      this.getMerkleTree(t).invalidate(t);
   }

   public MerkleTree getMerkleTree(Range<Token> range) {
      return (MerkleTree)this.merkleTrees.get(range);
   }

   public long size() {
      long size = 0L;

      MerkleTree tree;
      for(Iterator var3 = this.merkleTrees.values().iterator(); var3.hasNext(); size += tree.size()) {
         tree = (MerkleTree)var3.next();
      }

      return size;
   }

   @VisibleForTesting
   public void maxsize(Range<Token> range, int maxsize) {
      this.getMerkleTree(range).maxsize((long)maxsize);
   }

   private MerkleTree getMerkleTree(Token t) {
      Iterator var2 = this.merkleTrees.keySet().iterator();

      Range range;
      do {
         if(!var2.hasNext()) {
            throw new AssertionError("Expected tree for token " + t);
         }

         range = (Range)var2.next();
      } while(!range.contains((RingPosition)t));

      return (MerkleTree)this.merkleTrees.get(range);
   }

   private void addTrees(Collection<MerkleTree> trees) {
      Iterator var2 = trees.iterator();

      while(var2.hasNext()) {
         MerkleTree tree = (MerkleTree)var2.next();
         this.addTree(tree);
      }

   }

   private void addTree(MerkleTree tree) {
      assert this.validateNonOverlapping(tree) : "Range [" + tree.fullRange + "] is intersecting an existing range";

      this.merkleTrees.put(tree.fullRange, tree);
   }

   private boolean validateNonOverlapping(MerkleTree tree) {
      Iterator var2 = this.merkleTrees.keySet().iterator();

      Range range;
      do {
         if(!var2.hasNext()) {
            return true;
         }

         range = (Range)var2.next();
      } while(!tree.fullRange.intersects(range));

      return false;
   }

   public MerkleTrees.TreeRangeIterator invalids() {
      return new MerkleTrees.TreeRangeIterator();
   }

   public void logRowCountPerLeaf(Logger logger) {
      Iterator var2 = this.merkleTrees.values().iterator();

      while(var2.hasNext()) {
         MerkleTree tree = (MerkleTree)var2.next();
         tree.histogramOfRowCountPerLeaf().log(logger);
      }

   }

   public void logRowSizePerLeaf(Logger logger) {
      Iterator var2 = this.merkleTrees.values().iterator();

      while(var2.hasNext()) {
         MerkleTree tree = (MerkleTree)var2.next();
         tree.histogramOfRowSizePerLeaf().log(logger);
      }

   }

   @VisibleForTesting
   public byte[] hash(Range<Token> range) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      boolean hashed = false;

      try {
         Iterator var4 = this.merkleTrees.keySet().iterator();

         while(var4.hasNext()) {
            Range<Token> rt = (Range)var4.next();
            if(rt.intersects(range)) {
               byte[] bytes = ((MerkleTree)this.merkleTrees.get(rt)).hash(range);
               if(bytes != null) {
                  baos.write(bytes);
                  hashed = true;
               }
            }
         }
      } catch (IOException var7) {
         throw new RuntimeException("Unable to append merkle tree hash to result");
      }

      return hashed?baos.toByteArray():null;
   }

   public Iterator<Entry<Range<Token>, MerkleTree>> iterator() {
      return this.merkleTrees.entrySet().iterator();
   }

   public long rowCount() {
      long totalCount = 0L;

      MerkleTree tree;
      for(Iterator var3 = this.merkleTrees.values().iterator(); var3.hasNext(); totalCount += tree.rowCount()) {
         tree = (MerkleTree)var3.next();
      }

      return totalCount;
   }

   public static List<Range<Token>> difference(MerkleTrees ltree, MerkleTrees rtree) {
      List<MerkleTree.TreeDifference> diff = diff(ltree, rtree);
      List<Range<Token>> result = new ArrayList();
      result.addAll(diff);
      return result;
   }

   public static List<MerkleTree.TreeDifference> diff(MerkleTrees ltree, MerkleTrees rtree) {
      List<MerkleTree.TreeDifference> differences = new ArrayList();
      Iterator var3 = ltree.merkleTrees.values().iterator();

      while(var3.hasNext()) {
         MerkleTree tree = (MerkleTree)var3.next();
         differences.addAll(MerkleTree.diff(tree, rtree.getMerkleTree(tree.fullRange)));
      }

      return differences;
   }

   private static class TokenRangeComparator implements Comparator<Range<Token>> {
      private TokenRangeComparator() {
      }

      public int compare(Range<Token> rt1, Range<Token> rt2) {
         return ((Token)rt1.left).compareTo(rt2.left) == 0?0:rt1.compareTo(rt2);
      }
   }

   public static class MerkleTreesSerializer extends VersionDependent<RepairVerbs.RepairVersion> implements Serializer<MerkleTrees> {
      private MerkleTreesSerializer(RepairVerbs.RepairVersion version) {
         super(version);
      }

      public void serialize(MerkleTrees trees, DataOutputPlus out) throws IOException {
         out.writeInt(trees.merkleTrees.size());
         Iterator var3 = trees.merkleTrees.values().iterator();

         while(var3.hasNext()) {
            MerkleTree tree = (MerkleTree)var3.next();
            ((MerkleTree.MerkleTreeSerializer)MerkleTree.serializers.get(this.version)).serialize(tree, out);
         }

      }

      public MerkleTrees deserialize(DataInputPlus in) throws IOException {
         IPartitioner partitioner = null;
         int nTrees = in.readInt();
         Collection<MerkleTree> trees = new ArrayList(nTrees);
         if(nTrees > 0) {
            for(int i = 0; i < nTrees; ++i) {
               MerkleTree tree = ((MerkleTree.MerkleTreeSerializer)MerkleTree.serializers.get(this.version)).deserialize(in);
               trees.add(tree);
               if(partitioner == null) {
                  partitioner = tree.partitioner();
               } else {
                  assert tree.partitioner() == partitioner;
               }
            }
         }

         return new MerkleTrees(partitioner, trees);
      }

      public long serializedSize(MerkleTrees trees) {
         assert trees != null;

         long size = (long)TypeSizes.sizeof(trees.merkleTrees.size());

         MerkleTree tree;
         for(Iterator var4 = trees.merkleTrees.values().iterator(); var4.hasNext(); size += ((MerkleTree.MerkleTreeSerializer)MerkleTree.serializers.get(this.version)).serializedSize(tree)) {
            tree = (MerkleTree)var4.next();
         }

         return size;
      }
   }

   public class TreeRangeIterator extends com.google.common.collect.AbstractIterator<MerkleTree.TreeRange> implements Iterable<MerkleTree.TreeRange>, PeekingIterator<MerkleTree.TreeRange> {
      private final Iterator<MerkleTree> it;
      private MerkleTree.TreeRangeIterator current;

      private TreeRangeIterator() {
         this.current = null;
         this.it = MerkleTrees.this.merkleTrees.values().iterator();
      }

      public MerkleTree.TreeRange computeNext() {
         return this.current != null && this.current.hasNext()?(MerkleTree.TreeRange)this.current.next():this.nextIterator();
      }

      private MerkleTree.TreeRange nextIterator() {
         if(this.it.hasNext()) {
            this.current = ((MerkleTree)this.it.next()).invalids();
            return (MerkleTree.TreeRange)this.current.next();
         } else {
            return (MerkleTree.TreeRange)this.endOfData();
         }
      }

      public Iterator<MerkleTree.TreeRange> iterator() {
         return this;
      }
   }
}
