package org.apache.cassandra.locator;

import java.net.InetAddress;
import org.apache.cassandra.net.Verb;

public interface ILatencySubscriber {
   void receiveTiming(Verb<?, ?> var1, InetAddress var2, long var3);
}
