package org.apache.cassandra.repair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DigestVersion;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Validator implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(Validator.class);
   private static DigestVersion DIGEST_VERSION = (DigestVersion)Version.last(DigestVersion.class);
   public final RepairJobDesc desc;
   public final InetAddress initiator;
   public final int nowInSec;
   private final boolean evenTreeDistribution;
   public final boolean isIncremental;
   private long validated;
   private MerkleTrees trees;
   private MerkleTree.TreeRange range;
   private MerkleTrees.TreeRangeIterator ranges;
   private DecoratedKey lastKey;
   private final PreviewKind previewKind;

   public Validator(RepairJobDesc desc, InetAddress initiator, int nowInSec, PreviewKind previewKind) {
      this(desc, initiator, nowInSec, false, false, previewKind);
   }

   public Validator(RepairJobDesc desc, InetAddress initiator, int nowInSec, boolean isIncremental, PreviewKind previewKind) {
      this(desc, initiator, nowInSec, false, isIncremental, previewKind);
   }

   public Validator(RepairJobDesc desc, InetAddress initiator, int nowInSec, boolean evenTreeDistribution, boolean isIncremental, PreviewKind previewKind) {
      this.desc = desc;
      this.initiator = initiator;
      this.nowInSec = nowInSec;
      this.isIncremental = isIncremental;
      this.previewKind = previewKind;
      this.validated = 0L;
      this.range = null;
      this.ranges = null;
      this.evenTreeDistribution = evenTreeDistribution;
   }

   public void prepare(ColumnFamilyStore cfs, MerkleTrees tree) {
      this.trees = tree;
      if(tree.partitioner().preservesOrder() && !this.evenTreeDistribution) {
         List<DecoratedKey> keys = new ArrayList();
         Random random = new Random();
         Iterator var5 = tree.ranges().iterator();

         label46:
         while(true) {
            while(true) {
               if(!var5.hasNext()) {
                  break label46;
               }

               Range<Token> range = (Range)var5.next();
               Iterator var7 = cfs.keySamples(range).iterator();

               DecoratedKey dk;
               while(var7.hasNext()) {
                  dk = (DecoratedKey)var7.next();

                  assert range.contains((RingPosition)dk.getToken()) : "Token " + dk.getToken() + " is not within range " + this.desc.ranges;

                  keys.add(dk);
               }

               if(keys.isEmpty()) {
                  tree.init(range);
               } else {
                  int numKeys = keys.size();

                  do {
                     dk = (DecoratedKey)keys.get(random.nextInt(numKeys));
                  } while(tree.split(dk.getToken()));

                  keys.clear();
               }
            }
         }
      } else {
         tree.init();
      }

      logger.debug("Prepared AEService trees of size {} for {}", Long.valueOf(this.trees.size()), this.desc);
      this.ranges = tree.invalids();
   }

   public void add(UnfilteredRowIterator partition) {
      assert Range.isInRanges(partition.partitionKey().getToken(), this.desc.ranges) : partition.partitionKey().getToken() + " is not contained in " + this.desc.ranges;

      assert this.lastKey == null || this.lastKey.compareTo((PartitionPosition)partition.partitionKey()) < 0 : "partition " + partition.partitionKey() + " received out of order wrt " + this.lastKey;

      this.lastKey = partition.partitionKey();
      if(this.range == null) {
         this.range = (MerkleTree.TreeRange)this.ranges.next();
      }

      if(!this.findCorrectRange(this.lastKey.getToken())) {
         this.ranges = this.trees.invalids();
         this.findCorrectRange(this.lastKey.getToken());
      }

      assert this.range.contains(this.lastKey.getToken()) : "Token not in MerkleTree: " + this.lastKey.getToken();

      MerkleTree.RowHash rowHash = this.rowHash(partition);
      if(rowHash != null) {
         this.range.addHash(rowHash);
      }

   }

   public boolean findCorrectRange(Token t) {
      while(!this.range.contains(t) && this.ranges.hasNext()) {
         this.range = (MerkleTree.TreeRange)this.ranges.next();
      }

      return this.range.contains(t);
   }

   private MerkleTree.RowHash rowHash(UnfilteredRowIterator partition) {
      ++this.validated;
      Validator.CountingHasher hasher = new Validator.CountingHasher(Hashing.sha256().newHasher());
      UnfilteredRowIterators.digest((UnfilteredRowIterator)partition, hasher, DIGEST_VERSION);
      return hasher.count > 0L?new MerkleTree.RowHash(partition.partitionKey().getToken(), hasher.hash().asBytes(), hasher.count):null;
   }

   public void complete() {
      this.completeTree();
      StageManager.getStage(Stage.ANTI_ENTROPY).execute(this);
      if(logger.isDebugEnabled()) {
         logger.debug("Validated {} partitions for {}.  Partitions per leaf are:", Long.valueOf(this.validated), this.desc.sessionId);
         this.trees.logRowCountPerLeaf(logger);
         logger.debug("Validated {} partitions for {}.  Partition sizes are:", Long.valueOf(this.validated), this.desc.sessionId);
         this.trees.logRowSizePerLeaf(logger);
      }

   }

   @VisibleForTesting
   public void completeTree() {
      assert this.ranges != null : "Validator was not prepared()";

      this.ranges = this.trees.invalids();

      while(this.ranges.hasNext()) {
         this.range = (MerkleTree.TreeRange)this.ranges.next();
         this.range.ensureHashInitialised();
      }

   }

   public void fail() {
      logger.error("Failed creating a merkle tree for {}, {} (see log for details)", this.desc, this.initiator);
      MessagingService.instance().send(Verbs.REPAIR.VALIDATION_COMPLETE.newRequest(this.initiator, (Object)(new ValidationComplete(this.desc))));
   }

   public void run() {
      if(!this.initiator.equals(FBUtilities.getBroadcastAddress())) {
         logger.info("{} Sending completed merkle tree to {} for {}.{}", new Object[]{this.previewKind.logPrefix(this.desc.sessionId), this.initiator, this.desc.keyspace, this.desc.columnFamily});
         Tracing.traceRepair("Sending completed merkle tree to {} for {}.{}", new Object[]{this.initiator, this.desc.keyspace, this.desc.columnFamily});
      }

      MessagingService.instance().send(Verbs.REPAIR.VALIDATION_COMPLETE.newRequest(this.initiator, (Object)(new ValidationComplete(this.desc, this.trees))));
   }

   static class CountingHasher implements Hasher {
      private long count;
      private final Hasher underlying;

      CountingHasher(Hasher underlying) {
         this.underlying = underlying;
      }

      public Hasher putByte(byte b) {
         ++this.count;
         return this.underlying.putByte(b);
      }

      public Hasher putBytes(byte[] bytes) {
         this.count += (long)bytes.length;
         return this.underlying.putBytes(bytes);
      }

      public Hasher putBytes(byte[] bytes, int offset, int length) {
         this.count += (long)length;
         return this.underlying.putBytes(bytes, offset, length);
      }

      public Hasher putShort(short i) {
         this.count += 2L;
         return this.underlying.putShort(i);
      }

      public Hasher putInt(int i) {
         this.count += 4L;
         return this.underlying.putInt(i);
      }

      public Hasher putLong(long l) {
         this.count += 8L;
         return this.underlying.putLong(l);
      }

      public Hasher putFloat(float v) {
         this.count += 4L;
         return this.underlying.putFloat(v);
      }

      public Hasher putDouble(double v) {
         this.count += 8L;
         return this.underlying.putDouble(v);
      }

      public Hasher putBoolean(boolean b) {
         ++this.count;
         return this.underlying.putBoolean(b);
      }

      public Hasher putChar(char c) {
         this.count += 2L;
         return this.underlying.putChar(c);
      }

      public Hasher putUnencodedChars(CharSequence charSequence) {
         throw new UnsupportedOperationException();
      }

      public Hasher putString(CharSequence charSequence, Charset charset) {
         throw new UnsupportedOperationException();
      }

      public <T> Hasher putObject(T t, Funnel<? super T> funnel) {
         throw new UnsupportedOperationException();
      }

      public HashCode hash() {
         return this.underlying.hash();
      }
   }
}
