package org.apache.cassandra.notifications;

public interface INotificationConsumer {
   void handleNotification(INotification var1, Object var2);
}
