package org.apache.cassandra.service;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.DeferredFlow;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadCallback<T> implements MessageCallback<ReadResponse> {
   protected static final Logger logger = LoggerFactory.getLogger(ReadCallback.class);
   final ResponseResolver<T> resolver;
   final List<InetAddress> endpoints;
   private final int blockfor;
   private final AtomicInteger received = new AtomicInteger(0);
   private final AtomicInteger failures = new AtomicInteger(0);
   private final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;
   private final DeferredFlow<T> result;
   private final Supplier<Consumer<Flow<T>>> notification;
   private volatile Consumer<Flow<T>> notificationAction;

   private ReadCallback(ResponseResolver<T> resolver, List<InetAddress> endpoints) {
      this.resolver = resolver;
      this.endpoints = endpoints;
      this.blockfor = resolver.ctx.blockFor(endpoints);
      this.failureReasonByEndpoint = new ConcurrentHashMap();
      long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(this.command().getTimeout());
      this.notification = () -> {
         return this.notificationAction;
      };
      this.result = DeferredFlow.create(this.queryStartNanos() + timeoutNanos, resolver.command.getSchedulerSupplier(), this::generateFlowOnTimeout, this.notification);
      if(logger.isTraceEnabled()) {
         logger.trace("Blockfor is {}; setting up requests to {}", Integer.valueOf(this.blockfor), StringUtils.join(this.endpoints, ","));
      }

      if(this.readContext().readObserver != null) {
         this.readContext().readObserver.queried(endpoints);
      }

   }

   static <T> ReadCallback<T> forResolver(ResponseResolver<T> resolver, List<InetAddress> targets) {
      return new ReadCallback(resolver, targets);
   }

   public static ReadCallback<FlowablePartition> forInitialRead(ReadCommand command, List<InetAddress> targets, ReadContext ctx) {
      return forResolver((ResponseResolver)(ctx.withDigests?new DigestResolver(command, ctx, targets.size()):new DataResolver(command, ctx, targets.size())), targets);
   }

   Pair<ReadCallback<FlowablePartition>, Collection<InetAddress>> forDigestMismatchRepair(List<InetAddress> targets) {
      assert this.resolver instanceof DigestResolver;

      DigestResolver digestResolver = (DigestResolver)this.resolver;

      assert digestResolver.isDataPresent();

      Response<ReadResponse> dataResponse = digestResolver.dataResponse;
      ReadCallback<FlowablePartition> callback = forResolver(new DataResolver(this.command(), this.readContext(), targets.size()), targets);
      callback.onResponse(dataResponse);
      return Pair.create(callback, this.subtractTarget(targets, dataResponse.from()));
   }

   private List<InetAddress> subtractTarget(List<InetAddress> targets, InetAddress toSubstract) {
      assert !targets.isEmpty() : "We shouldn't have got a mismatch with no targets";

      List<InetAddress> toQuery = new ArrayList(targets.size() - 1);
      Iterator var4 = targets.iterator();

      while(var4.hasNext()) {
         InetAddress target = (InetAddress)var4.next();
         if(!target.equals(toSubstract)) {
            toQuery.add(target);
         }
      }

      return toQuery;
   }

   ReadCommand command() {
      return this.resolver.command;
   }

   ReadContext readContext() {
      return this.resolver.ctx;
   }

   private ConsistencyLevel consistency() {
      return this.resolver.consistency();
   }

   private long queryStartNanos() {
      return this.readContext().queryStartNanos;
   }

   public Flow<T> result() {
      return this.result;
   }

   boolean hasResult() {
      return this.result.hasSource();
   }

   void onResult(Consumer<Flow<T>> action) {
      this.notificationAction = action;
   }

   public int blockFor() {
      return this.blockfor;
   }

   private Flow<T> generateFlowOnSuccess(int receivedResponses) {
      if(this.readContext().readObserver != null) {
         this.readContext().readObserver.responsesReceived((Collection)(receivedResponses == this.endpoints.size()?this.endpoints:ImmutableSet.copyOf(Iterables.transform(this.resolver.getMessages(), Message::from))));
      }

      try {
         return (this.blockfor == 1?this.resolver.getData():this.resolver.resolve()).doOnError(this::onError);
      } catch (Throwable var3) {
         if(logger.isTraceEnabled()) {
            logger.trace("Got error: {}/{}", var3.getClass().getName(), var3.getMessage());
         }

         return Flow.error(var3);
      }
   }

   private Flow<T> generateFlowOnTimeout() {
      int responses = this.received.get();
      int requiredResponses = this.readContext().requiredResponses();
      return responses >= requiredResponses && this.resolver.isDataPresent()?this.generateFlowOnSuccess(responses):Flow.error(new ReadTimeoutException(this.consistency(), responses, this.blockfor, this.resolver.isDataPresent()));
   }

   public void onResponse(Response<ReadResponse> message) {
      if(logger.isTraceEnabled()) {
         logger.trace("Received response: {}", message);
      }

      this.resolver.preprocess(message);
      int n = this.waitingFor(message.from())?this.received.incrementAndGet():this.received.get();
      if(n >= this.blockfor && this.resolver.isDataPresent()) {
         if(this.result.onSource(this.generateFlowOnSuccess(n)) && logger.isTraceEnabled()) {
            logger.trace("Read: {} ms.", Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.queryStartNanos())));
         }

         if(this.blockfor < this.endpoints.size() && n == this.endpoints.size()) {
            TraceState traceState = Tracing.instance.get();
            if(traceState != null) {
               traceState.trace("Initiating read-repair");
            }

            if(logger.isTraceEnabled()) {
               logger.trace("Initiating read-repair");
            }

            StageManager.getStage(Stage.READ_REPAIR).execute(new ReadCallback.AsyncRepairRunner(traceState));
         }
      }

   }

   public void onTimeout(InetAddress host) {
      this.result.onSource(this.generateFlowOnTimeout());
   }

   private boolean waitingFor(InetAddress from) {
      return !this.consistency().isDatacenterLocal() || DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(from);
   }

   void assureSufficientLiveNodes() throws UnavailableException {
      this.consistency().assureSufficientLiveNodes(this.readContext().keyspace, this.endpoints);
   }

   public void onFailure(FailureResponse<ReadResponse> failureResponse) {
      if(logger.isTraceEnabled()) {
         logger.trace("Received failure response: {}", failureResponse);
      }

      int n = this.waitingFor(failureResponse.from())?this.failures.incrementAndGet():this.failures.get();
      this.failureReasonByEndpoint.put(failureResponse.from(), failureResponse.reason());
      if(this.blockfor + n > this.endpoints.size() && !this.result.hasSource()) {
         this.result.onSource(Flow.error(new ReadFailureException(this.consistency(), this.received.get(), this.blockfor, this.resolver.isDataPresent(), this.failureReasonByEndpoint)));
      }

   }

   private void onError(Throwable error) {
      int received = this.received.get();
      boolean isTimeout = error instanceof ReadTimeoutException;
      boolean isFailure = error instanceof ReadFailureException;
      if(!isTimeout && !isFailure) {
         if(!(error instanceof DigestMismatchException) && !(error instanceof UnavailableException)) {
            logger.error("Unexpected error handling read responses for {}. Have received {} of {} responses.", new Object[]{this.resolver.command, Integer.valueOf(received), Integer.valueOf(this.blockfor), error});
         }
      } else {
         String gotData;
         if(Tracing.isTracing()) {
            gotData = received > 0?(this.resolver.isDataPresent()?" (including data)":" (only digests)"):"";
            Tracing.trace("{}; received {} of {} responses{}", new Object[]{isFailure?"Failed":"Timed out", Integer.valueOf(received), Integer.valueOf(this.blockfor), gotData});
         } else if(logger.isDebugEnabled()) {
            gotData = received > 0?(this.resolver.isDataPresent()?" (including data)":" (only digests)"):"";
            logger.debug("{}; received {} of {} responses{}", new Object[]{isFailure?"Failed":"Timed out", Integer.valueOf(received), Integer.valueOf(this.blockfor), gotData});
         }
      }

   }

   private class AsyncRepairRunner implements Runnable {
      private final TraceState traceState;

      private AsyncRepairRunner(TraceState traceState) {
         this.traceState = traceState;
      }

      public void run() {
         try {
            ReadCallback.this.resolver.compareResponses().blockingAwait();
         } catch (Throwable var2) {
            Throwable e = var2;
            if(var2 instanceof RuntimeException && var2.getCause() != null) {
               e = var2.getCause();
            }

            if(!(e instanceof DigestMismatchException)) {
               throw Throwables.propagate(e);
            }

            this.retryOnDigestMismatch((DigestMismatchException)e);
         }

      }

      private void retryOnDigestMismatch(DigestMismatchException e) {
         assert ReadCallback.this.resolver instanceof DigestResolver;

         if(this.traceState != null) {
            this.traceState.trace("Digest mismatch: {}", (Object)e.toString());
         }

         if(ReadCallback.logger.isTraceEnabled()) {
            ReadCallback.logger.trace("Digest mismatch: {}", e.toString());
         }

         ReadRepairMetrics.repairedBackground.mark();
         Response<ReadResponse> dataResponse = ((DigestResolver)ReadCallback.this.resolver).dataResponse;

         assert dataResponse != null;

         DataResolver repairResolver = new DataResolver(ReadCallback.this.command(), ReadCallback.this.readContext(), ReadCallback.this.endpoints.size());
         AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, ReadCallback.this.endpoints.size());
         repairHandler.onResponse(dataResponse);
         MessagingService.instance().send((Request.Dispatcher)ReadCallback.this.command().dispatcherTo(ReadCallback.this.subtractTarget(ReadCallback.this.endpoints, dataResponse.from())), repairHandler);
      }
   }
}
