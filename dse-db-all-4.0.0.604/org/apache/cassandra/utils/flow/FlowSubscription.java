package org.apache.cassandra.utils.flow;

public interface FlowSubscription extends AutoCloseable {
   FlowSubscription DONE = new FlowSubscription() {
      public void requestNext() {
         throw new AssertionError("Already completed.");
      }

      public void close() throws Exception {
      }
   };

   void requestNext();

   void close() throws Exception;
}
