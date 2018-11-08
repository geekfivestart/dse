package org.apache.cassandra.concurrent;

public enum TPCTaskType {
   UNKNOWN,
   FRAME_DECODE(5),
   READ_LOCAL(21),
   READ_REMOTE(149),
   READ_TIMEOUT(8),
   READ_DEFERRED(5),
   READ_INTERNAL(4),
   READ_RESPONSE("READ_SWITCH_FOR_RESPONSE"),
   READ_RANGE_LOCAL(21),
   READ_RANGE_REMOTE(149),
   READ_RANGE_NODESYNC(5),
   READ_RANGE_INTERNAL(4),
   READ_RANGE_RESPONSE("READ_RANGE_SWITCH_FOR_RESPONSE"),
   READ_FROM_ITERATOR("READ_SWITCH_FOR_ITERATOR"),
   READ_SECONDARY_INDEX,
   READ_DISK_ASYNC(98),
   WRITE_LOCAL(21),
   WRITE_REMOTE(149),
   WRITE_INTERNAL(4),
   WRITE_RESPONSE("WRITE_SWITCH_FOR_RESPONSE"),
   WRITE_DEFRAGMENT(5),
   WRITE_MEMTABLE("WRITE_SWITCH_FOR_MEMTABLE"),
   WRITE_POST_COMMIT_LOG_SEGMENT("WRITE_AWAIT_COMMITLOG_SEGMENT", 2),
   WRITE_POST_COMMIT_LOG_SYNC("WRITE_AWAIT_COMMITLOG_SYNC", 2),
   WRITE_POST_MEMTABLE_FULL("WRITE_MEMTABLE_FULL", 2),
   BATCH_REPLAY(4),
   BATCH_STORE(5),
   BATCH_STORE_RESPONSE,
   BATCH_REMOVE(5),
   COUNTER_ACQUIRE_LOCK(4),
   EXECUTE_STATEMENT(4),
   CONTINUOUS_PAGING(4),
   CAS(4),
   LWT_PREPARE(5),
   LWT_PROPOSE(5),
   LWT_COMMIT(5),
   TRUNCATE(5),
   NODESYNC_VALIDATION(4),
   AUTHENTICATION(4),
   AUTHORIZATION(68),
   READ_SPECULATE(4),
   TIMED_TIMEOUT(40),
   TIMED_UNKNOWN(8),
   EVENTLOOP_SPIN(8),
   EVENTLOOP_YIELD(8),
   EVENTLOOP_PARK(8),
   HINT_DISPATCH(5),
   HINT_RESPONSE,
   NETWORK_BACKPRESSURE(21);

   private static final int PENDABLE = 1;
   private static final int EXTERNAL_QUEUE = 2;
   private static final int ALWAYS_COUNT = 4;
   private static final int EXCLUDE_FROM_TOTALS = 8;
   private static final int BACKPRESSURED = 16;
   private static final int PRIORITY = 32;
   private static final int ALWAYS_ENQUEUE = 64;
   private static final int REMOTE = 128;
   private final int flags;
   public final String loggedEventName;

   public final boolean pendable() {
      return (this.flags & 1) != 0;
   }

   public final boolean backpressured() {
      return (this.flags & 16) != 0;
   }

   public final boolean externalQueue() {
      return (this.flags & 2) != 0;
   }

   public final boolean logIfExecutedImmediately() {
      return (this.flags & 4) != 0;
   }

   public final boolean includedInTotals() {
      return (this.flags & 8) == 0;
   }

   public final boolean priority() {
      return (this.flags & 32) != 0;
   }

   public final boolean alwaysEnqueue() {
      return (this.flags & 64) != 0;
   }

   public final boolean remote() {
      return (this.flags & 128) != 0;
   }

   private TPCTaskType(String loggedEventName, int flags) {
      this.loggedEventName = loggedEventName != null?loggedEventName:this.name();
      this.flags = flags;
   }

   private TPCTaskType(int flags) {
      this((String)null, flags);
   }

   private TPCTaskType(String eventName) {
      this(eventName, 0);
   }

   private TPCTaskType() {
      this((String)null, 0);
   }

   private static class Features {
      static final int PENDABLE = 5;
      static final int BACKPRESSURED = 21;
      static final int ALWAYS_COUNT = 4;
      static final int EXTERNAL_QUEUE = 2;
      static final int TIMED = 8;
      static final int TIMER = 40;
      static final int EXCLUDE_FROM_TOTALS = 8;
      static final int ALWAYS_ENQUEUE = 64;
      static final int PRIORITY = 32;
      static final int REMOTE = 128;

      private Features() {
      }
   }
}
