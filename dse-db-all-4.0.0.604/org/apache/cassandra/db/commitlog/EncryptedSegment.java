package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.crypto.Cipher;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.SyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedSegment extends FileDirectSegment {
   private static final Logger logger = LoggerFactory.getLogger(EncryptedSegment.class);
   private static final int ENCRYPTED_SECTION_HEADER_SIZE = 12;
   private final EncryptionContext encryptionContext;
   private final Cipher cipher;

   public EncryptedSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager) {
      super(commitLog, manager);
      this.encryptionContext = commitLog.configuration.getEncryptionContext();

      try {
         this.cipher = this.encryptionContext.getEncryptor();
      } catch (IOException var4) {
         throw new FSWriteError(var4, this.logFile);
      }

      logger.debug("created a new encrypted commit log segment: {}", this.logFile);
      manager.getBufferPool().setPreferredReusableBufferType(BufferType.ON_HEAP);
   }

   protected Map<String, String> additionalHeaderParameters() {
      Map<String, String> map = this.encryptionContext.toHeaderParameters();
      map.put("encIV", Hex.bytesToHex(this.cipher.getIV()));
      return map;
   }

   ByteBuffer createBuffer(CommitLog commitLog) {
      return this.manager.getBufferPool().createBuffer(BufferType.ON_HEAP);
   }

   synchronized void write(int startMarker, int nextMarker) {
      int contentStart = startMarker + 8;
      int length = nextMarker - contentStart;

      assert length > 0 || length == 0 && !this.isStillAllocating();

      ICompressor compressor = this.encryptionContext.getCompressor();
      int blockSize = this.encryptionContext.getChunkLength();

      try {
         ByteBuffer inputBuffer = this.buffer.duplicate();
         inputBuffer.limit(contentStart + length).position(contentStart);
         ByteBuffer buffer = this.manager.getBufferPool().getThreadLocalReusableBuffer(DatabaseDescriptor.getCommitLogSegmentSize());
         long syncMarkerPosition = this.lastWrittenPos;
         this.channel.position(syncMarkerPosition + 12L);

         while(contentStart < nextMarker) {
            int nextBlockSize = nextMarker - blockSize > contentStart?blockSize:nextMarker - contentStart;
            ByteBuffer slice = inputBuffer.duplicate();
            slice.limit(contentStart + nextBlockSize).position(contentStart);
            buffer = EncryptionUtils.compress(slice, buffer, true, compressor);
            buffer = EncryptionUtils.encryptAndWrite(buffer, this.channel, true, this.cipher);
            contentStart += nextBlockSize;
            this.manager.addSize((long)(buffer.limit() + 8));
         }

         this.lastWrittenPos = this.channel.position();
         buffer.position(0).limit(12);
         writeSyncMarker(this.id, buffer, 0, (int)syncMarkerPosition, (int)this.lastWrittenPos);
         buffer.putInt(8, length);
         buffer.rewind();
         this.manager.addSize((long)buffer.limit());
         this.channel.position(syncMarkerPosition);
         this.channel.write(buffer);
         SyncUtil.force(this.channel, true);
      } catch (Exception var13) {
         throw new FSWriteError(var13, this.getPath());
      }
   }

   public long onDiskSize() {
      return this.lastWrittenPos;
   }
}
