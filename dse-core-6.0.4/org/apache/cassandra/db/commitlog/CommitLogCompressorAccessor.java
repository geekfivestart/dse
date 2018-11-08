package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.io.compress.ICompressor;

public class CommitLogCompressorAccessor {
   public CommitLogCompressorAccessor() {
   }

   public static Class<? extends ICompressor> getCommitLogCompressorClass() {
      ICompressor compressor = CommitLog.instance.configuration.getCompressor();
      return compressor != null?compressor.getClass():null;
   }
}
