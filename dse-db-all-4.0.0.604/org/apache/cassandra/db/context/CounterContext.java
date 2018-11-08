package org.apache.cassandra.db.context;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.db.ClockAndCount;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.HashingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterContext {
   private static final int HEADER_SIZE_LENGTH = TypeSizes.sizeof((short)32767);
   private static final int HEADER_ELT_LENGTH = TypeSizes.sizeof((short)32767);
   private static final int CLOCK_LENGTH = TypeSizes.sizeof(9223372036854775807L);
   private static final int COUNT_LENGTH = TypeSizes.sizeof(9223372036854775807L);
   private static final int STEP_LENGTH;
   static final CounterId UPDATE_CLOCK_ID;
   private static final Logger logger;

   public CounterContext() {
   }

   public static CounterContext instance() {
      return CounterContext.LazyHolder.counterContext;
   }

   public ByteBuffer createUpdate(long count) {
      CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 1, 0);
      state.writeLocal(UPDATE_CLOCK_ID, 1L, count);
      return state.context;
   }

   public boolean isUpdate(ByteBuffer context) {
      return CounterContext.ContextState.wrap(context).getCounterId().equals(UPDATE_CLOCK_ID);
   }

   public ByteBuffer createGlobal(CounterId id, long clock, long count) {
      CounterContext.ContextState state = CounterContext.ContextState.allocate(1, 0, 0);
      state.writeGlobal(id, clock, count);
      return state.context;
   }

   public ByteBuffer createLocal(long count) {
      CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 1, 0);
      state.writeLocal(CounterId.getLocalId(), 1L, count);
      return state.context;
   }

   public ByteBuffer createRemote(CounterId id, long clock, long count) {
      CounterContext.ContextState state = CounterContext.ContextState.allocate(0, 0, 1);
      state.writeRemote(id, clock, count);
      return state.context;
   }

   private static int headerLength(ByteBuffer context) {
      return HEADER_SIZE_LENGTH + Math.abs(context.getShort(context.position())) * HEADER_ELT_LENGTH;
   }

   private static int compareId(ByteBuffer bb1, int pos1, ByteBuffer bb2, int pos2) {
      return ByteBufferUtil.compareSubArrays(bb1, pos1, bb2, pos2, 16);
   }

   public CounterContext.Relationship diff(ByteBuffer left, ByteBuffer right) {
      CounterContext.Relationship relationship = CounterContext.Relationship.EQUAL;
      CounterContext.ContextState leftState = CounterContext.ContextState.wrap(left);
      CounterContext.ContextState rightState = CounterContext.ContextState.wrap(right);

      while(leftState.hasRemaining() && rightState.hasRemaining()) {
         int compareId = leftState.compareIdTo(rightState);
         if(compareId == 0) {
            long leftClock = leftState.getClock();
            long rightClock = rightState.getClock();
            long leftCount = leftState.getCount();
            long rightCount = rightState.getCount();
            leftState.moveToNext();
            rightState.moveToNext();
            if(leftClock == rightClock) {
               if(leftCount != rightCount) {
                  return CounterContext.Relationship.DISJOINT;
               }
            } else if((leftClock < 0L || rightClock <= 0L || leftClock <= rightClock) && (leftClock >= 0L || rightClock <= 0L && leftClock >= rightClock)) {
               if(relationship == CounterContext.Relationship.EQUAL) {
                  relationship = CounterContext.Relationship.LESS_THAN;
               } else if(relationship == CounterContext.Relationship.GREATER_THAN) {
                  return CounterContext.Relationship.DISJOINT;
               }
            } else if(relationship == CounterContext.Relationship.EQUAL) {
               relationship = CounterContext.Relationship.GREATER_THAN;
            } else if(relationship == CounterContext.Relationship.LESS_THAN) {
               return CounterContext.Relationship.DISJOINT;
            }
         } else if(compareId > 0) {
            rightState.moveToNext();
            if(relationship == CounterContext.Relationship.EQUAL) {
               relationship = CounterContext.Relationship.LESS_THAN;
            } else if(relationship == CounterContext.Relationship.GREATER_THAN) {
               return CounterContext.Relationship.DISJOINT;
            }
         } else {
            leftState.moveToNext();
            if(relationship == CounterContext.Relationship.EQUAL) {
               relationship = CounterContext.Relationship.GREATER_THAN;
            } else if(relationship == CounterContext.Relationship.LESS_THAN) {
               return CounterContext.Relationship.DISJOINT;
            }
         }
      }

      if(leftState.hasRemaining()) {
         if(relationship == CounterContext.Relationship.EQUAL) {
            return CounterContext.Relationship.GREATER_THAN;
         }

         if(relationship == CounterContext.Relationship.LESS_THAN) {
            return CounterContext.Relationship.DISJOINT;
         }
      }

      if(rightState.hasRemaining()) {
         if(relationship == CounterContext.Relationship.EQUAL) {
            return CounterContext.Relationship.LESS_THAN;
         }

         if(relationship == CounterContext.Relationship.GREATER_THAN) {
            return CounterContext.Relationship.DISJOINT;
         }
      }

      return relationship;
   }

   public ByteBuffer merge(ByteBuffer left, ByteBuffer right) {
      boolean leftIsSuperSet = true;
      boolean rightIsSuperSet = true;
      int globalCount = 0;
      int localCount = 0;
      int remoteCount = 0;
      CounterContext.ContextState leftState = CounterContext.ContextState.wrap(left);
      CounterContext.ContextState rightState = CounterContext.ContextState.wrap(right);

      while(leftState.hasRemaining() && rightState.hasRemaining()) {
         int cmp = leftState.compareIdTo(rightState);
         if(cmp == 0) {
            CounterContext.Relationship rel = this.compare(leftState, rightState);
            if(rel == CounterContext.Relationship.GREATER_THAN) {
               rightIsSuperSet = false;
            } else if(rel == CounterContext.Relationship.LESS_THAN) {
               leftIsSuperSet = false;
            } else if(rel == CounterContext.Relationship.DISJOINT) {
               rightIsSuperSet = false;
               leftIsSuperSet = false;
            }

            if(!leftState.isGlobal() && !rightState.isGlobal()) {
               if(!leftState.isLocal() && !rightState.isLocal()) {
                  ++remoteCount;
               } else {
                  ++localCount;
               }
            } else {
               ++globalCount;
            }

            leftState.moveToNext();
            rightState.moveToNext();
         } else if(cmp > 0) {
            leftIsSuperSet = false;
            if(rightState.isGlobal()) {
               ++globalCount;
            } else if(rightState.isLocal()) {
               ++localCount;
            } else {
               ++remoteCount;
            }

            rightState.moveToNext();
         } else {
            rightIsSuperSet = false;
            if(leftState.isGlobal()) {
               ++globalCount;
            } else if(leftState.isLocal()) {
               ++localCount;
            } else {
               ++remoteCount;
            }

            leftState.moveToNext();
         }
      }

      if(leftState.hasRemaining()) {
         rightIsSuperSet = false;
      } else if(rightState.hasRemaining()) {
         leftIsSuperSet = false;
      }

      if(leftIsSuperSet) {
         return left;
      } else if(rightIsSuperSet) {
         return right;
      } else {
         for(; leftState.hasRemaining(); leftState.moveToNext()) {
            if(leftState.isGlobal()) {
               ++globalCount;
            } else if(leftState.isLocal()) {
               ++localCount;
            } else {
               ++remoteCount;
            }
         }

         for(; rightState.hasRemaining(); rightState.moveToNext()) {
            if(rightState.isGlobal()) {
               ++globalCount;
            } else if(rightState.isLocal()) {
               ++localCount;
            } else {
               ++remoteCount;
            }
         }

         leftState.reset();
         rightState.reset();
         return this.merge(CounterContext.ContextState.allocate(globalCount, localCount, remoteCount), leftState, rightState);
      }
   }

   private ByteBuffer merge(CounterContext.ContextState mergedState, CounterContext.ContextState leftState, CounterContext.ContextState rightState) {
      while(leftState.hasRemaining() && rightState.hasRemaining()) {
         int cmp = leftState.compareIdTo(rightState);
         if(cmp == 0) {
            CounterContext.Relationship rel = this.compare(leftState, rightState);
            if(rel == CounterContext.Relationship.DISJOINT) {
               mergedState.writeLocal(leftState.getCounterId(), leftState.getClock() + rightState.getClock(), leftState.getCount() + rightState.getCount());
            } else if(rel == CounterContext.Relationship.GREATER_THAN) {
               leftState.copyTo(mergedState);
            } else {
               rightState.copyTo(mergedState);
            }

            rightState.moveToNext();
            leftState.moveToNext();
         } else if(cmp > 0) {
            rightState.copyTo(mergedState);
            rightState.moveToNext();
         } else {
            leftState.copyTo(mergedState);
            leftState.moveToNext();
         }
      }

      while(leftState.hasRemaining()) {
         leftState.copyTo(mergedState);
         leftState.moveToNext();
      }

      while(rightState.hasRemaining()) {
         rightState.copyTo(mergedState);
         rightState.moveToNext();
      }

      return mergedState.context;
   }

   private CounterContext.Relationship compare(CounterContext.ContextState leftState, CounterContext.ContextState rightState) {
      long leftClock = leftState.getClock();
      long leftCount = leftState.getCount();
      long rightClock = rightState.getClock();
      long rightCount = rightState.getCount();
      if(!leftState.isGlobal() && !rightState.isGlobal()) {
         if(!leftState.isLocal() && !rightState.isLocal()) {
            if(leftClock == rightClock) {
               if(leftCount != rightCount && ((Boolean)CompactionManager.isCompactionManager.get()).booleanValue()) {
                  logger.warn("invalid remote counter shard detected; ({}, {}, {}) and ({}, {}, {}) differ only in count; will pick highest to self-heal on compaction", new Object[]{leftState.getCounterId(), Long.valueOf(leftClock), Long.valueOf(leftCount), rightState.getCounterId(), Long.valueOf(rightClock), Long.valueOf(rightCount)});
               }

               return leftCount > rightCount?CounterContext.Relationship.GREATER_THAN:(leftCount == rightCount?CounterContext.Relationship.EQUAL:CounterContext.Relationship.LESS_THAN);
            } else {
               return (leftClock < 0L || rightClock <= 0L || leftClock < rightClock) && (leftClock >= 0L || rightClock <= 0L && leftClock >= rightClock)?CounterContext.Relationship.LESS_THAN:CounterContext.Relationship.GREATER_THAN;
            }
         } else {
            return leftState.isLocal() && rightState.isLocal()?CounterContext.Relationship.DISJOINT:(leftState.isLocal()?CounterContext.Relationship.GREATER_THAN:CounterContext.Relationship.LESS_THAN);
         }
      } else if(leftState.isGlobal() && rightState.isGlobal()) {
         if(leftClock == rightClock) {
            if(leftCount != rightCount && ((Boolean)CompactionManager.isCompactionManager.get()).booleanValue()) {
               logger.warn("invalid global counter shard detected; ({}, {}, {}) and ({}, {}, {}) differ only in count; will pick highest to self-heal on compaction", new Object[]{leftState.getCounterId(), Long.valueOf(leftClock), Long.valueOf(leftCount), rightState.getCounterId(), Long.valueOf(rightClock), Long.valueOf(rightCount)});
            }

            return leftCount > rightCount?CounterContext.Relationship.GREATER_THAN:(leftCount == rightCount?CounterContext.Relationship.EQUAL:CounterContext.Relationship.LESS_THAN);
         } else {
            return leftClock > rightClock?CounterContext.Relationship.GREATER_THAN:CounterContext.Relationship.LESS_THAN;
         }
      } else {
         return leftState.isGlobal()?CounterContext.Relationship.GREATER_THAN:CounterContext.Relationship.LESS_THAN;
      }
   }

   public String toString(ByteBuffer context) {
      CounterContext.ContextState state = CounterContext.ContextState.wrap(context);
      StringBuilder sb = new StringBuilder();
      sb.append("[");

      for(; state.hasRemaining(); state.moveToNext()) {
         if(state.getElementIndex() > 0) {
            sb.append(",");
         }

         sb.append("{");
         sb.append(state.getCounterId()).append(", ");
         sb.append(state.getClock()).append(", ");
         sb.append(state.getCount());
         sb.append("}");
         if(state.isGlobal()) {
            sb.append("$");
         } else if(state.isLocal()) {
            sb.append("*");
         }
      }

      sb.append("]");
      return sb.toString();
   }

   public long total(ByteBuffer context) {
      long total = 0L;

      for(int offset = context.position() + headerLength(context); offset < context.limit(); offset += STEP_LENGTH) {
         total += context.getLong(offset + 16 + CLOCK_LENGTH);
      }

      return total;
   }

   public boolean shouldClearLocal(ByteBuffer context) {
      return context.getShort(context.position()) < 0;
   }

   public boolean hasLegacyShards(ByteBuffer context) {
      int totalCount = (context.remaining() - headerLength(context)) / STEP_LENGTH;
      int localAndGlobalCount = Math.abs(context.getShort(context.position()));
      if(localAndGlobalCount < totalCount) {
         return true;
      } else {
         for(int i = 0; i < localAndGlobalCount; ++i) {
            if(context.getShort(context.position() + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH) >= 0) {
               return true;
            }
         }

         return false;
      }
   }

   public ByteBuffer markLocalToBeCleared(ByteBuffer context) {
      short count = context.getShort(context.position());
      if(count <= 0) {
         return context;
      } else {
         boolean hasLocalShards = false;

         for(int i = 0; i < count; ++i) {
            if(context.getShort(context.position() + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH) >= 0) {
               hasLocalShards = true;
               break;
            }
         }

         if(!hasLocalShards) {
            return context;
         } else {
            ByteBuffer marked = ByteBuffer.allocate(context.remaining());
            marked.putShort(marked.position(), (short)(count * -1));
            ByteBufferUtil.arrayCopy(context, context.position() + HEADER_SIZE_LENGTH, marked, marked.position() + HEADER_SIZE_LENGTH, context.remaining() - HEADER_SIZE_LENGTH);
            return marked;
         }
      }
   }

   public ByteBuffer clearAllLocal(ByteBuffer context) {
      int count = Math.abs(context.getShort(context.position()));
      if(count == 0) {
         return context;
      } else {
         List<Short> globalShardIndexes = new ArrayList(count);

         for(int i = 0; i < count; ++i) {
            short elt = context.getShort(context.position() + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH);
            if(elt < 0) {
               globalShardIndexes.add(Short.valueOf(elt));
            }
         }

         if(count == globalShardIndexes.size()) {
            return context;
         } else {
            ByteBuffer cleared = ByteBuffer.allocate(context.remaining() - (count - globalShardIndexes.size()) * HEADER_ELT_LENGTH);
            cleared.putShort(cleared.position(), (short)globalShardIndexes.size());

            int origHeaderLength;
            for(origHeaderLength = 0; origHeaderLength < globalShardIndexes.size(); ++origHeaderLength) {
               cleared.putShort(cleared.position() + HEADER_SIZE_LENGTH + origHeaderLength * HEADER_ELT_LENGTH, ((Short)globalShardIndexes.get(origHeaderLength)).shortValue());
            }

            origHeaderLength = headerLength(context);
            ByteBufferUtil.arrayCopy(context, context.position() + origHeaderLength, cleared, cleared.position() + headerLength(cleared), context.remaining() - origHeaderLength);
            return cleared;
         }
      }
   }

   public void validateContext(ByteBuffer context) throws MarshalException {
      if((context.remaining() - headerLength(context)) % STEP_LENGTH != 0) {
         throw new MarshalException("Invalid size for a counter context");
      }
   }

   public void updateDigest(Hasher hasher, ByteBuffer context) {
      if(context.hasRemaining()) {
         ByteBuffer dup = context.duplicate();
         dup.position(context.position() + headerLength(context));
         HashingUtils.updateBytes(hasher, dup);
      }
   }

   public ClockAndCount getLocalClockAndCount(ByteBuffer context) {
      return this.getClockAndCountOf(context, CounterId.getLocalId());
   }

   public long getLocalCount(ByteBuffer context) {
      return this.getLocalClockAndCount(context).count;
   }

   @VisibleForTesting
   public ClockAndCount getClockAndCountOf(ByteBuffer context, CounterId id) {
      int position = this.findPositionOf(context, id);
      if(position == -1) {
         return ClockAndCount.BLANK;
      } else {
         long clock = context.getLong(position + 16);
         long count = context.getLong(position + 16 + CLOCK_LENGTH);
         return ClockAndCount.create(clock, count);
      }
   }

   @VisibleForTesting
   public int findPositionOf(ByteBuffer context, CounterId id) {
      int headerLength = headerLength(context);
      int offset = context.position() + headerLength;
      int left = 0;
      int right = (context.remaining() - headerLength) / STEP_LENGTH - 1;

      while(right >= left) {
         int middle = (left + right) / 2;
         int cmp = compareId(context, offset + middle * STEP_LENGTH, id.bytes(), id.bytes().position());
         if(cmp == -1) {
            left = middle + 1;
         } else {
            if(cmp == 0) {
               return offset + middle * STEP_LENGTH;
            }

            right = middle - 1;
         }
      }

      return -1;
   }

   static {
      STEP_LENGTH = 16 + CLOCK_LENGTH + COUNT_LENGTH;
      UPDATE_CLOCK_ID = CounterId.fromInt(0);
      logger = LoggerFactory.getLogger(CounterContext.class);
   }

   public static class ContextState {
      public final ByteBuffer context;
      public final int headerLength;
      private int headerOffset;
      private int bodyOffset;
      private boolean currentIsGlobal;
      private boolean currentIsLocal;

      private ContextState(ByteBuffer context) {
         this.context = context;
         this.headerLength = this.bodyOffset = CounterContext.headerLength(context);
         this.headerOffset = CounterContext.HEADER_SIZE_LENGTH;
         this.updateIsGlobalOrLocal();
      }

      public static CounterContext.ContextState wrap(ByteBuffer context) {
         return new CounterContext.ContextState(context);
      }

      public static CounterContext.ContextState allocate(int globalCount, int localCount, int remoteCount) {
         int headerLength = CounterContext.HEADER_SIZE_LENGTH + (globalCount + localCount) * CounterContext.HEADER_ELT_LENGTH;
         int bodyLength = (globalCount + localCount + remoteCount) * CounterContext.STEP_LENGTH;
         ByteBuffer buffer = ByteBuffer.allocate(headerLength + bodyLength);
         buffer.putShort(buffer.position(), (short)(globalCount + localCount));
         return wrap(buffer);
      }

      public boolean isGlobal() {
         return this.currentIsGlobal;
      }

      public boolean isLocal() {
         return this.currentIsLocal;
      }

      public boolean isRemote() {
         return !this.currentIsGlobal && !this.currentIsLocal;
      }

      private void updateIsGlobalOrLocal() {
         if(this.headerOffset >= this.headerLength) {
            this.currentIsGlobal = this.currentIsLocal = false;
         } else {
            short headerElt = this.context.getShort(this.context.position() + this.headerOffset);
            this.currentIsGlobal = headerElt == this.getElementIndex() + -32768;
            this.currentIsLocal = headerElt == this.getElementIndex();
         }

      }

      public boolean hasRemaining() {
         return this.bodyOffset < this.context.remaining();
      }

      public void moveToNext() {
         this.bodyOffset += CounterContext.STEP_LENGTH;
         if(this.currentIsGlobal || this.currentIsLocal) {
            this.headerOffset += CounterContext.HEADER_ELT_LENGTH;
         }

         this.updateIsGlobalOrLocal();
      }

      public void copyTo(CounterContext.ContextState other) {
         other.writeElement(this.getCounterId(), this.getClock(), this.getCount(), this.currentIsGlobal, this.currentIsLocal);
      }

      public int compareIdTo(CounterContext.ContextState other) {
         return CounterContext.compareId(this.context, this.context.position() + this.bodyOffset, other.context, other.context.position() + other.bodyOffset);
      }

      public void reset() {
         this.headerOffset = CounterContext.HEADER_SIZE_LENGTH;
         this.bodyOffset = this.headerLength;
         this.updateIsGlobalOrLocal();
      }

      public int getElementIndex() {
         return (this.bodyOffset - this.headerLength) / CounterContext.STEP_LENGTH;
      }

      public CounterId getCounterId() {
         return CounterId.wrap(this.context, this.context.position() + this.bodyOffset);
      }

      public long getClock() {
         return this.context.getLong(this.context.position() + this.bodyOffset + 16);
      }

      public long getCount() {
         return this.context.getLong(this.context.position() + this.bodyOffset + 16 + CounterContext.CLOCK_LENGTH);
      }

      public void writeGlobal(CounterId id, long clock, long count) {
         this.writeElement(id, clock, count, true, false);
      }

      public void writeLocal(CounterId id, long clock, long count) {
         this.writeElement(id, clock, count, false, true);
      }

      public void writeRemote(CounterId id, long clock, long count) {
         this.writeElement(id, clock, count, false, false);
      }

      private void writeElement(CounterId id, long clock, long count, boolean isGlobal, boolean isLocal) {
         this.writeElementAtOffset(this.context, this.context.position() + this.bodyOffset, id, clock, count);
         if(isGlobal) {
            this.context.putShort(this.context.position() + this.headerOffset, (short)(this.getElementIndex() + -32768));
         } else if(isLocal) {
            this.context.putShort(this.context.position() + this.headerOffset, (short)this.getElementIndex());
         }

         this.currentIsGlobal = isGlobal;
         this.currentIsLocal = isLocal;
         this.moveToNext();
      }

      private void writeElementAtOffset(ByteBuffer ctx, int offset, CounterId id, long clock, long count) {
         ctx = ctx.duplicate();
         ctx.position(offset);
         ctx.put(id.bytes().duplicate());
         ctx.putLong(clock);
         ctx.putLong(count);
      }
   }

   private static class LazyHolder {
      private static final CounterContext counterContext = new CounterContext();

      private LazyHolder() {
      }
   }

   public static enum Relationship {
      EQUAL,
      GREATER_THAN,
      LESS_THAN,
      DISJOINT;

      private Relationship() {
      }
   }
}
