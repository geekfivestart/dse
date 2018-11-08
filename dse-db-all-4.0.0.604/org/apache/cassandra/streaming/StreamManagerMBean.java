package org.apache.cassandra.streaming;

import java.util.Set;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;

public interface StreamManagerMBean extends NotificationEmitter {
   String OBJECT_NAME = "org.apache.cassandra.net:type=StreamManager";

   Set<CompositeData> getCurrentStreams();
}
