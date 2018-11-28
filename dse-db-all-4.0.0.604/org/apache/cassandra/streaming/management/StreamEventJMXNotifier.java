package org.apache.cassandra.streaming.management;

import java.util.concurrent.atomic.AtomicLong;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;

public class StreamEventJMXNotifier extends NotificationBroadcasterSupport implements StreamEventHandler {
   private static final long PROGRESS_NOTIFICATION_INTERVAL = 1000L;
   private final AtomicLong seq = new AtomicLong();
   private long progressLastSent;

   public StreamEventJMXNotifier() {
   }

   public void handleStreamEvent(StreamEvent event) {
      Notification notif = null;
      switch (event.eventType) {
         case STREAM_PREPARED: {
            notif = new Notification(StreamEvent.SessionPreparedEvent.class.getCanonicalName(), "org.apache.cassandra.net:type=StreamManager", this.seq.getAndIncrement());
            notif.setUserData(SessionInfoCompositeData.toCompositeData(event.planId, ((StreamEvent.SessionPreparedEvent)event).session));
            break;
         }
         case STREAM_COMPLETE: {
            notif = new Notification(StreamEvent.SessionCompleteEvent.class.getCanonicalName(), "org.apache.cassandra.net:type=StreamManager", this.seq.getAndIncrement());
            notif.setUserData(SessionCompleteEventCompositeData.toCompositeData((StreamEvent.SessionCompleteEvent)event));
            break;
         }
         case FILE_PROGRESS: {
            ProgressInfo progress = ((StreamEvent.ProgressEvent)event).progress;
            long current = System.currentTimeMillis();
            if (current - this.progressLastSent >= 1000L || progress.isCompleted()) {
               notif = new Notification(StreamEvent.ProgressEvent.class.getCanonicalName(), "org.apache.cassandra.net:type=StreamManager", this.seq.getAndIncrement());
               notif.setUserData(ProgressInfoCompositeData.toCompositeData(event.planId, progress));
               this.progressLastSent = System.currentTimeMillis();
               break;
            }
            return;
         }
      }
      this.sendNotification(notif);
   }


   public void onSuccess(StreamState result) {
      Notification notif = new Notification(StreamEvent.class.getCanonicalName() + ".success", "org.apache.cassandra.net:type=StreamManager", this.seq.getAndIncrement());
      notif.setUserData(StreamStateCompositeData.toCompositeData(result));
      this.sendNotification(notif);
   }

   public void onFailure(Throwable t) {
      Notification notif = new Notification(StreamEvent.class.getCanonicalName() + ".failure", "org.apache.cassandra.net:type=StreamManager", this.seq.getAndIncrement());
      notif.setUserData(t.fillInStackTrace().toString());
      this.sendNotification(notif);
   }
}
