package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.zip.CRC32;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OpOrderSimple;

final class HintsBuffer {
   static final int ENTRY_OVERHEAD_SIZE = 12;
   static final int CLOSED = -1;
   private final ByteBuffer slab;
   private final AtomicInteger position;
   private final ConcurrentMap<UUID, Queue<Integer>> offsets;
   private final OpOrder appendOrder;

   private HintsBuffer(ByteBuffer slab) {
      this.slab = slab;
      this.position = new AtomicInteger();
      this.offsets = new ConcurrentHashMap();
      this.appendOrder = new OpOrderSimple();
   }

   static HintsBuffer create(int slabSize) {
      return new HintsBuffer(ByteBuffer.allocateDirect(slabSize));
   }

   boolean isClosed() {
      return this.position.get() == -1;
   }

   int capacity() {
      return this.slab.capacity();
   }

   int remaining() {
      int pos = this.position.get();
      return pos == -1?0:this.capacity() - pos;
   }

   HintsBuffer recycle() {
      this.slab.clear();
      return new HintsBuffer(this.slab);
   }

   void free() {
      FileUtils.clean(this.slab);
   }

   void waitForModifications() {
      this.appendOrder.awaitNewBarrier();
   }

   Set<UUID> hostIds() {
      return this.offsets.keySet();
   }

   Iterator<ByteBuffer> consumingHintsIterator(UUID hostId) {
      final Queue<Integer> bufferOffsets = (Queue)this.offsets.get(hostId);
      return (Iterator)(bufferOffsets == null?Collections.emptyIterator():new AbstractIterator<ByteBuffer>() {
         private final ByteBuffer flyweight;

         {
            this.flyweight = HintsBuffer.this.slab.duplicate();
         }

         protected ByteBuffer computeNext() {
            Integer offset = (Integer)bufferOffsets.poll();
            if(offset == null) {
               return (ByteBuffer)this.endOfData();
            } else {
               int totalSize = HintsBuffer.this.slab.getInt(offset.intValue()) + 12;
               return (ByteBuffer)this.flyweight.clear().position(offset.intValue()).limit(offset.intValue() + totalSize);
            }
         }
      });
   }

   HintsBuffer.Allocation allocate(int hintSize) {
      int totalSize = hintSize + 12;
      if(totalSize > this.slab.capacity() / 2) {
         throw new IllegalArgumentException(String.format("Hint of %s bytes is too large - the maximum size is %s", new Object[]{Integer.valueOf(hintSize), Integer.valueOf(this.slab.capacity() / 2)}));
      } else {
         OpOrder.Group opGroup = this.appendOrder.start();

         try {
            return this.allocate(totalSize, opGroup);
         } catch (Throwable var5) {
            opGroup.close();
            throw var5;
         }
      }
   }

   private HintsBuffer.Allocation allocate(int totalSize, OpOrder.Group opGroup) {
      int offset = this.allocateBytes(totalSize);
      if(offset < 0) {
         opGroup.close();
         return null;
      } else {
         return new HintsBuffer.Allocation(offset, totalSize, opGroup);
      }
   }

   private int allocateBytes(int totalSize) {
      int prev;
      int next;
      do {
         prev = this.position.get();
         next = prev + totalSize;
         if(prev == -1) {
            return -1;
         }

         if(next > this.slab.capacity()) {
            this.position.set(-1);
            return -1;
         }
      } while(!this.position.compareAndSet(prev, next));

      return prev;
   }

   private void put(UUID hostId, int offset) {
      Queue<Integer> queue = (Queue)this.offsets.get(hostId);
      if(queue == null) {
         queue = (Queue)this.offsets.computeIfAbsent(hostId, (id) -> {
            return new ConcurrentLinkedQueue();
         });
      }

      queue.offer(Integer.valueOf(offset));
   }

   final class Allocation implements AutoCloseable {
      private final Integer offset;
      private final int totalSize;
      private final OpOrder.Group opGroup;

      Allocation(int offset, int totalSize, OpOrder.Group opGroup) {
         this.offset = Integer.valueOf(offset);
         this.totalSize = totalSize;
         this.opGroup = opGroup;
      }

      void write(Iterable<UUID> hostIds, Hint hint) {
         this.write(hint);
         Iterator var3 = hostIds.iterator();

         while(var3.hasNext()) {
            UUID hostId = (UUID)var3.next();
            HintsBuffer.this.put(hostId, this.offset.intValue());
         }

      }

      public void close() {
         this.opGroup.close();
      }

      private void write(Hint hint) {
         ByteBuffer buffer = (ByteBuffer)HintsBuffer.this.slab.duplicate().position(this.offset.intValue()).limit(this.offset.intValue() + this.totalSize);
         CRC32 crc = new CRC32();
         int hintSize = this.totalSize - 12;

         try {
            DataOutputBuffer dop = new DataOutputBufferFixed(buffer);
            Throwable var6 = null;

            try {
               dop.writeInt(hintSize);
               FBUtilities.updateChecksumInt(crc, hintSize);
               dop.writeInt((int)crc.getValue());
               ((Hint.HintSerializer)Hint.serializers.get(HintsDescriptor.CURRENT_VERSION)).serialize((Hint)hint, dop);
               FBUtilities.updateChecksum(crc, buffer, buffer.position() - hintSize, hintSize);
               dop.writeInt((int)crc.getValue());
            } catch (Throwable var16) {
               var6 = var16;
               throw var16;
            } finally {
               if(dop != null) {
                  if(var6 != null) {
                     try {
                        dop.close();
                     } catch (Throwable var15) {
                        var6.addSuppressed(var15);
                     }
                  } else {
                     dop.close();
                  }
               }

            }

         } catch (IOException var18) {
            throw new AssertionError();
         }
      }
   }
}
