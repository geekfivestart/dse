package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteSource;

class RowIndexWriter {
   ByteSource prevMax = null;
   ByteSource prevSep = null;
   final ClusteringComparator comparator;
   final IncrementalTrieWriter<RowIndexReader.IndexInfo> trie;

   public RowIndexWriter(ClusteringComparator comparator, DataOutputPlus out) {
      this.trie = IncrementalTrieWriter.construct(RowIndexReader.trieSerializer, out);
      this.comparator = comparator;
   }

   void add(ClusteringPrefix firstName, ClusteringPrefix lastName, RowIndexReader.IndexInfo info) throws IOException {
      assert info.openDeletion != null;

      ByteSource sep;
      if(this.prevMax == null) {
         sep = ByteSource.empty();
      } else {
         ByteSource currMin = this.comparator.asByteComparableSource(firstName);
         sep = ByteSource.separatorGt(this.prevMax, currMin);
      }

      this.trie.add(sep, info);
      this.prevSep = sep;
      this.prevMax = this.comparator.asByteComparableSource(lastName);
   }

   public long complete(long endPos) throws IOException {
      int i = 0;
      this.prevMax.reset();
      this.prevSep.reset();

      while(this.prevMax.next() == this.prevSep.next()) {
         ++i;
      }

      this.trie.add(this.nudge(this.prevMax, i), new RowIndexReader.IndexInfo(endPos, DeletionTime.LIVE));
      return this.trie.complete();
   }

   private ByteSource nudge(final ByteSource v, final int nudgeAt) {
      v.reset();
      return new ByteSource() {
         int cur = 0;

         public int next() {
            int b = -1;
            if(this.cur <= nudgeAt) {
               b = v.next();
               if(this.cur == nudgeAt) {
                  if(b >= 255) {
                     return b;
                  }

                  ++b;
               }
            }

            ++this.cur;
            return b;
         }

         public void reset() {
            this.cur = 0;
            v.reset();
         }
      };
   }
}
