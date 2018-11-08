package org.apache.cassandra.concurrent;

import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;

public interface ExecutorLocal<T> {
   ExecutorLocal[] all = new ExecutorLocal[]{Tracing.instance, ClientWarn.instance};

   T get();

   void set(T var1);
}
