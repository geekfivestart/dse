package org.apache.cassandra.concurrent;

import io.reactivex.disposables.Disposable;
import java.util.concurrent.TimeUnit;

public interface TPCTimer {
   TPCScheduler getScheduler();

   Disposable onTimeout(Runnable var1, long var2, TimeUnit var4);
}
