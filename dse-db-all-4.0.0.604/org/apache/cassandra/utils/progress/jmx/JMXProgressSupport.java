package org.apache.cassandra.utils.progress.jmx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressListener;

public class JMXProgressSupport implements ProgressListener {
   private final AtomicLong notificationSerialNumber = new AtomicLong();
   private final NotificationBroadcasterSupport broadcaster;

   public JMXProgressSupport(NotificationBroadcasterSupport broadcaster) {
      this.broadcaster = broadcaster;
   }

   public void progress(String tag, ProgressEvent event) {
      Notification notification = new Notification("progress", tag, this.notificationSerialNumber.getAndIncrement(), System.currentTimeMillis(), event.getMessage());
      Map<String, Integer> userData = new HashMap();
      userData.put("type", Integer.valueOf(event.getType().ordinal()));
      userData.put("progressCount", Integer.valueOf(event.getProgressCount()));
      userData.put("total", Integer.valueOf(event.getTotal()));
      notification.setUserData(userData);
      this.broadcaster.sendNotification(notification);
   }
}
