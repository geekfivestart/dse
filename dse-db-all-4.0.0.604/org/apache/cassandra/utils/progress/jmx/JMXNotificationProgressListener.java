package org.apache.cassandra.utils.progress.jmx;

import java.util.Map;
import javax.management.Notification;
import javax.management.NotificationListener;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

public abstract class JMXNotificationProgressListener implements ProgressListener, NotificationListener {
   public JMXNotificationProgressListener() {
   }

   public abstract boolean isInterestedIn(String var1);

   public void handleNotificationLost(long timestamp, String message) {
   }

   public void handleConnectionClosed(long timestamp, String message) {
   }

   public void handleConnectionFailed(long timestamp, String message) {
   }

   public void handleNotification(Notification notification, Object handback) {
      String var3 = notification.getType();
      byte var4 = -1;
      switch(var3.hashCode()) {
      case -1001078227:
         if(var3.equals("progress")) {
            var4 = 0;
         }
         break;
      case -739658258:
         if(var3.equals("jmx.remote.connection.notifs.lost")) {
            var4 = 1;
         }
         break;
      case -411860211:
         if(var3.equals("jmx.remote.connection.closed")) {
            var4 = 3;
         }
         break;
      case -336316962:
         if(var3.equals("jmx.remote.connection.failed")) {
            var4 = 2;
         }
      }

      switch(var4) {
      case 0:
         String tag = (String)notification.getSource();
         if(this.isInterestedIn(tag)) {
            Map<String, Integer> progress = (Map)notification.getUserData();
            String message = notification.getMessage();
            ProgressEvent event = new ProgressEvent(ProgressEventType.values()[((Integer)progress.get("type")).intValue()], ((Integer)progress.get("progressCount")).intValue(), ((Integer)progress.get("total")).intValue(), message);
            this.progress(tag, event);
         }
         break;
      case 1:
         this.handleNotificationLost(notification.getTimeStamp(), notification.getMessage());
         break;
      case 2:
         this.handleConnectionFailed(notification.getTimeStamp(), notification.getMessage());
         break;
      case 3:
         this.handleConnectionClosed(notification.getTimeStamp(), notification.getMessage());
      }

   }
}
