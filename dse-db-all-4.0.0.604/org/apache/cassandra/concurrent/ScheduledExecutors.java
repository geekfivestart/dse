package org.apache.cassandra.concurrent;

public class ScheduledExecutors {
   public static final DebuggableScheduledThreadPoolExecutor scheduledFastTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledFastTasks");
   public static final DebuggableScheduledThreadPoolExecutor scheduledTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledTasks");
   public static final DebuggableScheduledThreadPoolExecutor nonPeriodicTasks = new DebuggableScheduledThreadPoolExecutor("NonPeriodicTasks");
   public static final DebuggableScheduledThreadPoolExecutor optionalTasks = new DebuggableScheduledThreadPoolExecutor("OptionalTasks");

   public ScheduledExecutors() {
   }
}
