package org.apache.cassandra.tracing;

import com.google.common.base.Stopwatch;
import io.reactivex.Completable;
import io.reactivex.functions.Action;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressListener;
import org.slf4j.helpers.MessageFormatter;

public abstract class TraceState implements ProgressEventNotifier {
   public final UUID sessionId;
   public final InetAddress coordinator;
   public final Stopwatch watch;
   public final ByteBuffer sessionIdBytes;
   public final Tracing.TraceType traceType;
   public final int ttl;
   private boolean notify;
   private final List<ProgressListener> listeners = new CopyOnWriteArrayList();
   private String tag;
   private volatile TraceState.Status status;
   private final AtomicInteger references = new AtomicInteger(1);

   protected TraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType) {
      assert coordinator != null;

      assert sessionId != null;

      this.coordinator = coordinator;
      this.sessionId = sessionId;
      this.sessionIdBytes = ByteBufferUtil.bytes(sessionId);
      this.traceType = traceType;
      this.ttl = traceType.getTTL();
      this.watch = Stopwatch.createStarted();
      this.status = TraceState.Status.IDLE;
   }

   public void enableActivityNotification(String tag) {
      assert this.traceType == Tracing.TraceType.REPAIR;

      this.notify = true;
      this.tag = tag;
   }

   public void addProgressListener(ProgressListener listener) {
      assert this.traceType == Tracing.TraceType.REPAIR;

      this.listeners.add(listener);
   }

   public void removeProgressListener(ProgressListener listener) {
      assert this.traceType == Tracing.TraceType.REPAIR;

      this.listeners.remove(listener);
   }

   public int elapsed() {
      long elapsed = this.watch.elapsed(TimeUnit.MICROSECONDS);
      return elapsed < 2147483647L?(int)elapsed:2147483647;
   }

   public Completable stop() {
      return this.waitForPendingEvents().doOnComplete(() -> {
         synchronized(this) {
            this.status = TraceState.Status.STOPPED;
            this.notifyAll();
         }
      });
   }

   public synchronized TraceState.Status waitActivity(long timeout) {
      if(this.status == TraceState.Status.IDLE) {
         try {
            this.wait(timeout);
         } catch (InterruptedException var4) {
            throw new RuntimeException();
         }
      }

      if(this.status == TraceState.Status.ACTIVE) {
         this.status = TraceState.Status.IDLE;
         return TraceState.Status.ACTIVE;
      } else {
         return this.status;
      }
   }

   protected synchronized void notifyActivity() {
      this.status = TraceState.Status.ACTIVE;
      this.notifyAll();
   }

   public void trace(String format, Object arg) {
      this.trace(MessageFormatter.format(format, arg).getMessage());
   }

   public void trace(String format, Object arg1, Object arg2) {
      this.trace(MessageFormatter.format(format, arg1, arg2).getMessage());
   }

   public void trace(String format, Object... args) {
      this.trace(MessageFormatter.arrayFormat(format, args).getMessage());
   }

   public void trace(String message) {
      if(this.notify) {
         this.notifyActivity();
      }

      this.traceImpl(message);
      Iterator var2 = this.listeners.iterator();

      while(var2.hasNext()) {
         ProgressListener listener = (ProgressListener)var2.next();
         listener.progress(this.tag, ProgressEvent.createNotification(message));
      }

   }

   protected abstract void traceImpl(String var1);

   protected Completable waitForPendingEvents() {
      return Completable.complete();
   }

   public boolean acquireReference() {
      int n;
      do {
         n = this.references.get();
         if(n <= 0) {
            return false;
         }
      } while(!this.references.compareAndSet(n, n + 1));

      return true;
   }

   public int releaseReference() {
      return this.references.decrementAndGet();
   }

   public static enum Status {
      IDLE,
      ACTIVE,
      STOPPED;

      private Status() {
      }
   }
}
