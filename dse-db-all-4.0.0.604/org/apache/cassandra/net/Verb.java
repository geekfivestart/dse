package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.ExecutorSupplier;
import org.apache.cassandra.utils.TimeoutSupplier;
import org.apache.cassandra.utils.versioning.Version;

public abstract class Verb<P, Q> {
   final Consumer<Response<Q>> EMPTY_RESPONSE_CONSUMER = (r) -> {
   };
   private final Verb.Info<P> info;
   private final TimeoutSupplier<P> timeoutSupplier;
   private final VerbHandler<P, Q> handler;

   protected Verb(Verb.Info<P> info, TimeoutSupplier<P> timeoutSupplier, VerbHandler<P, Q> handler) {
      this.info = info;
      this.timeoutSupplier = timeoutSupplier;
      this.handler = handler;

      assert this.isOneWay() == (timeoutSupplier == null) : "One-way verbs must not have a timeout supplier, but other verbs must";

      assert info == null || this.isOneWay() == (info.droppedGroup == null) : "One-way verbs must not have a dropped group, but other verbs must";

   }

   public <V extends Enum<V> & Version<V>> VerbGroup<V> group() {
      return this.info.group;
   }

   public String name() {
      return this.info.name;
   }

   int groupIdx() {
      return this.info.groupIdx;
   }

   public TimeoutSupplier<P> timeoutSupplier() {
      return this.timeoutSupplier;
   }

   ExecutorSupplier<P> requestExecutor() {
      return this.info.requestExecutor;
   }

   ExecutorSupplier<P> responseExecutor() {
      return this.info.responseExecutor;
   }

   VerbHandler<P, Q> handler() {
      return this.handler;
   }

   ErrorHandler errorHandler() {
      return this.info.errorHandler;
   }

   boolean supportsBackPressure() {
      return this.info.supportsBackPressure;
   }

   DroppedMessages.Group droppedGroup() {
      return this.info.droppedGroup;
   }

   public abstract boolean isOneWay();

   private long computeRequestPayloadSize(P payload) {
      return MessagingService.current_version.serializer(this).requestSerializer.serializedSize(payload);
   }

   Message.Data<P> makeMessageData(P payload, boolean skipPayloadSizeComputation) {
      return new Message.Data(payload, skipPayloadSizeComputation?-1L:this.computeRequestPayloadSize(payload), System.currentTimeMillis(), this.timeoutSupplier == null?-1L:this.timeoutSupplier.get(payload));
   }

   public Request<P, Q> newRequest(InetAddress to, P payload) {
      return this.newRequest(to, this.makeMessageData(payload, to == Request.local));
   }

   Request<P, Q> newRequest(InetAddress to, Message.Data<P> messageData) {
      return new Request(Request.local, to, MessagingService.newMessageId(), this, messageData);
   }

   Request<P, Q> newRequestWithForwards(InetAddress to, Message.Data<P> messageData, List<InetAddress> forwards) {
      return new Request(Request.local, to, MessagingService.newMessageId(), this, messageData, Request.Forward.from(forwards));
   }

   boolean isOnlyLocal(Collection<InetAddress> endpoints) {
      return endpoints.size() == 1 && endpoints.iterator().next() == Request.local;
   }

   public Request.Dispatcher<P, Q> newDispatcher(Collection<InetAddress> endpoints, P payload) {
      return new Request.Dispatcher(MessageTargets.createSimple(endpoints), this, this.makeMessageData(payload, this.isOnlyLocal(endpoints)));
   }

   public Request.Dispatcher<P, Q> newForwardingDispatcher(Collection<InetAddress> endpoints, String localDc, P payload) {
      return new Request.Dispatcher(MessageTargets.createWithFowardingForRemoteDCs(endpoints, localDc), this, this.makeMessageData(payload, this.isOnlyLocal(endpoints)));
   }

   public final boolean equals(Object o) {
      return this == o;
   }

   public String toString() {
      return this.info.toString();
   }

   public static class AckedRequest<P> extends Verb.RequestResponse<P, EmptyPayload> {
      protected AckedRequest(Verb.Info<P> info, TimeoutSupplier<P> timeoutSupplier, VerbHandler<P, EmptyPayload> handler) {
         super(info, timeoutSupplier, handler);
      }

      public boolean isOneWay() {
         return false;
      }
   }

   public static class RequestResponse<P, Q> extends Verb<P, Q> {
      RequestResponse(Verb.Info<P> info, TimeoutSupplier<P> timeoutSupplier, VerbHandler<P, Q> handler) {
         super(info, timeoutSupplier, handler);
      }

      public boolean isOneWay() {
         return false;
      }
   }

   public static class OneWay<P> extends Verb<P, NoResponse> {
      OneWay(Verb.Info<P> info, VerbHandler<P, NoResponse> handler) {
         super(info, (TimeoutSupplier)null, handler);
      }

      public boolean isOneWay() {
         return true;
      }

      public OneWayRequest<P> newRequest(InetAddress to, P payload) {
         return this.newRequest(to, this.makeMessageData(payload, to == Request.local));
      }

      public OneWayRequest<P> newRequest(InetAddress to, Message.Data<P> messageData) {
         return new OneWayRequest(Request.local, to, this, messageData);
      }

      public OneWayRequest<P> newRequestWithForwards(InetAddress to, Message.Data<P> data, List<InetAddress> forwards) {
         return new OneWayRequest(Request.local, to, this, data, Request.Forward.from(forwards));
      }

      public OneWayRequest.Dispatcher<P> newDispatcher(Collection<InetAddress> endpoints, P payload) {
         return new OneWayRequest.Dispatcher(MessageTargets.createSimple(endpoints), this, this.makeMessageData(payload, this.isOnlyLocal(endpoints)));
      }
   }

   public static class Info<P> {
      private final VerbGroup<?> group;
      private final int groupIdx;
      private final String name;
      private final ExecutorSupplier<P> requestExecutor;
      private final ExecutorSupplier<P> responseExecutor;
      private final boolean supportsBackPressure;
      private final DroppedMessages.Group droppedGroup;
      private final ErrorHandler errorHandler;

      Info(VerbGroup<?> group, int groupIdx, String name, ExecutorSupplier<P> requestExecutor, ExecutorSupplier<P> responseExecutor, boolean supportsBackPressure, DroppedMessages.Group droppedGroup, ErrorHandler errorHandler) {
         assert group != null && name != null && requestExecutor != null && errorHandler != null;

         this.group = group;
         this.groupIdx = groupIdx;
         this.name = name;
         this.requestExecutor = requestExecutor;
         this.responseExecutor = responseExecutor;
         this.supportsBackPressure = supportsBackPressure;
         this.droppedGroup = droppedGroup;
         this.errorHandler = errorHandler;
      }

      public String toString() {
         return this.group.toString() + '.' + this.name;
      }
   }
}
