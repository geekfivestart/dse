package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.Condition;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;

public class BootstrapMonitor extends JMXNotificationProgressListener {
   private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
   private final PrintStream out;
   private final Condition condition = new SimpleCondition();

   public BootstrapMonitor(PrintStream out) {
      this.out = out;
   }

   public void awaitCompletion() throws InterruptedException {
      this.condition.await();
   }

   public boolean isInterestedIn(String tag) {
      return "bootstrap".equals(tag);
   }

   public void handleNotificationLost(long timestamp, String message) {
      super.handleNotificationLost(timestamp, message);
   }

   public void handleConnectionClosed(long timestamp, String message) {
      this.handleConnectionFailed(timestamp, message);
   }

   public void handleConnectionFailed(long timestamp, String message) {
      Exception error = new IOException(String.format("[%s] JMX connection closed. (%s)", new Object[]{this.format.format(Long.valueOf(timestamp)), message}));
      this.out.println(error.getMessage());
      this.condition.signalAll();
   }

   public void progress(String tag, ProgressEvent event) {
      ProgressEventType type = event.getType();
      String message = String.format("[%s] %s", new Object[]{this.format.format(Long.valueOf(System.currentTimeMillis())), event.getMessage()});
      if(type == ProgressEventType.PROGRESS) {
         message = message + " (progress: " + (int)event.getProgressPercentage() + "%)";
      }

      this.out.println(message);
      if(type == ProgressEventType.COMPLETE) {
         this.condition.signalAll();
      }

   }
}
