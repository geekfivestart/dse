package org.apache.cassandra.hints;

import com.google.common.util.concurrent.RateLimiter;
import java.io.File;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HintsDispatcher implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(HintsDispatcher.class);
   private final HintsReader reader;
   private final UUID hostId;
   private final InetAddress address;
   private final HintsVerbs.HintsVersion version;
   private final BooleanSupplier abortRequested;
   private InputPosition currentPagePosition = null;

   private HintsDispatcher(HintsReader reader, UUID hostId, InetAddress address, HintsVerbs.HintsVersion version, BooleanSupplier abortRequested) {
      this.reader = reader;
      this.hostId = hostId;
      this.address = address;
      this.version = version;
      this.abortRequested = abortRequested;
   }

   static HintsDispatcher create(File file, RateLimiter rateLimiter, InetAddress address, HintsVerbs.HintsVersion version, UUID hostId, BooleanSupplier abortRequested) {
      return new HintsDispatcher(HintsReader.open(file, rateLimiter), hostId, address, version, abortRequested);
   }

   public void close() {
      this.reader.close();
   }

   void seek(InputPosition position) {
      this.reader.seek(position);
   }

   boolean dispatch() {
      Iterator var1 = this.reader.iterator();

      HintsReader.Page page;
      do {
         if(!var1.hasNext()) {
            return true;
         }

         page = (HintsReader.Page)var1.next();
         this.currentPagePosition = page.position;
      } while(this.dispatch(page) == HintsDispatcher.Action.CONTINUE);

      return false;
   }

   InputPosition dispatchPosition() {
      return this.currentPagePosition;
   }

   private HintsDispatcher.Action dispatch(HintsReader.Page page) {
      return this.sendHintsAndAwait(page);
   }

   private HintsDispatcher.Action sendHintsAndAwait(HintsReader.Page page) {
      Collection<HintsDispatcher.Callback> callbacks = new ArrayList();
      HintsDispatcher.Action action = this.reader.descriptor().version == this.version && !this.address.equals(FBUtilities.getBroadcastAddress())?this.sendHints(page.buffersIterator(), callbacks, this::sendEncodedHint):this.sendHints(page.hintsIterator(), callbacks, this::sendHint);
      if(action == HintsDispatcher.Action.ABORT) {
         return action;
      } else {
         boolean hadFailures = false;
         Iterator var5 = callbacks.iterator();

         while(var5.hasNext()) {
            HintsDispatcher.Callback cb = (HintsDispatcher.Callback)var5.next();
            HintsDispatcher.Callback.Outcome outcome = cb.await();
            this.updateMetrics(outcome);
            if(outcome != HintsDispatcher.Callback.Outcome.SUCCESS) {
               hadFailures = true;
            }
         }

         return hadFailures?HintsDispatcher.Action.ABORT:HintsDispatcher.Action.CONTINUE;
      }
   }

   private void updateMetrics(HintsDispatcher.Callback.Outcome outcome) {
      switch(null.$SwitchMap$org$apache$cassandra$hints$HintsDispatcher$Callback$Outcome[outcome.ordinal()]) {
      case 1:
         HintsServiceMetrics.hintsSucceeded.mark();
         break;
      case 2:
         HintsServiceMetrics.hintsFailed.mark();
         break;
      case 3:
         HintsServiceMetrics.hintsTimedOut.mark();
      }

   }

   private <T> HintsDispatcher.Action sendHints(Iterator<T> hints, Collection<HintsDispatcher.Callback> callbacks, Function<T, HintsDispatcher.Callback> sendFunction) {
      while(hints.hasNext()) {
         if(this.abortRequested.getAsBoolean()) {
            return HintsDispatcher.Action.ABORT;
         }

         callbacks.add(sendFunction.apply(hints.next()));
      }

      return HintsDispatcher.Action.CONTINUE;
   }

   private HintsDispatcher.Callback sendHint(Hint hint) {
      HintsDispatcher.Callback callback = new HintsDispatcher.Callback(hint.creationTime, null);
      HintMessage message = HintMessage.create(this.hostId, hint);
      MessagingService.instance().send((Request)Verbs.HINTS.HINT.newRequest(this.address, message), callback);
      return callback;
   }

   private HintsDispatcher.Callback sendEncodedHint(ByteBuffer hint) {
      HintMessage message = HintMessage.createEncoded(this.hostId, hint, this.version);
      HintsDispatcher.Callback callback = new HintsDispatcher.Callback(message.getHintCreationTime(), null);
      MessagingService.instance().send((Request)Verbs.HINTS.HINT.newRequest(this.address, message), callback);
      return callback;
   }

   private static final class Callback implements MessageCallback<EmptyPayload> {
      private final long start;
      private final SimpleCondition condition;
      private volatile HintsDispatcher.Callback.Outcome outcome;
      private final long hintCreationTime;

      private Callback(long hintCreationTime) {
         this.start = System.nanoTime();
         this.condition = new SimpleCondition();
         this.hintCreationTime = hintCreationTime;
      }

      HintsDispatcher.Callback.Outcome await() {
         long timeout = TimeUnit.MILLISECONDS.toNanos(Verbs.HINTS.HINT.timeoutSupplier().get((Object)null)) - (System.nanoTime() - this.start);

         boolean timedOut;
         try {
            timedOut = !this.condition.await(timeout, TimeUnit.NANOSECONDS);
         } catch (InterruptedException var5) {
            HintsDispatcher.logger.warn("Hint dispatch was interrupted", var5);
            return HintsDispatcher.Callback.Outcome.INTERRUPTED;
         }

         return timedOut?HintsDispatcher.Callback.Outcome.TIMEOUT:this.outcome;
      }

      public void onFailure(FailureResponse<EmptyPayload> responseFailure) {
         this.outcome = HintsDispatcher.Callback.Outcome.FAILURE;
         this.condition.signalAll();
      }

      public void onResponse(Response<EmptyPayload> msg) {
         HintsServiceMetrics.updateDelayMetrics(msg.from(), ApproximateTime.currentTimeMillis() - this.hintCreationTime);
         this.outcome = HintsDispatcher.Callback.Outcome.SUCCESS;
         this.condition.signalAll();
      }

      public void onTimeout(InetAddress host) {
         this.outcome = HintsDispatcher.Callback.Outcome.TIMEOUT;
      }

      static enum Outcome {
         SUCCESS,
         TIMEOUT,
         FAILURE,
         INTERRUPTED;

         private Outcome() {
         }
      }
   }

   private static enum Action {
      CONTINUE,
      ABORT;

      private Action() {
      }
   }
}
