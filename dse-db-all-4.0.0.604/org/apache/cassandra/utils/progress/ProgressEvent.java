package org.apache.cassandra.utils.progress;

public class ProgressEvent {
   private final ProgressEventType type;
   private final int progressCount;
   private final int total;
   private final String message;

   public static ProgressEvent createNotification(String message) {
      return new ProgressEvent(ProgressEventType.NOTIFICATION, 0, 0, message);
   }

   public ProgressEvent(ProgressEventType type, int progressCount, int total) {
      this(type, progressCount, total, (String)null);
   }

   public ProgressEvent(ProgressEventType type, int progressCount, int total, String message) {
      this.type = type;
      this.progressCount = progressCount;
      this.total = total;
      this.message = message;
   }

   public ProgressEventType getType() {
      return this.type;
   }

   public int getProgressCount() {
      return this.progressCount;
   }

   public int getTotal() {
      return this.total;
   }

   public double getProgressPercentage() {
      return this.total != 0?(double)(this.progressCount * 100) / (double)this.total:0.0D;
   }

   public String getMessage() {
      return this.message;
   }
}
