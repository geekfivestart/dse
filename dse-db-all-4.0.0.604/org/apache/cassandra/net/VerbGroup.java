package org.apache.cassandra.net;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.ExecutorSupplier;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.TimeoutSupplier;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class VerbGroup<V extends Enum<V> & Version<V>> implements Iterable<Verb<?, ?>> {
   private static final Logger logger = LoggerFactory.getLogger(VerbGroup.class);
   private final Verbs.Group id;
   private final boolean isInternal;
   private final Class<V> versionClass;
   private final List<Verb<?, ?>> verbs = new ArrayList();
   private final EnumMap<V, VersionSerializers> versionedSerializers;
   private boolean helperCreated;

   protected VerbGroup(Verbs.Group id, boolean isInternal, Class<V> versionClass) {
      this.id = id;
      this.isInternal = isInternal;
      this.versionClass = versionClass;
      this.versionedSerializers = new EnumMap(versionClass);
      Enum[] var4 = (Enum[])versionClass.getEnumConstants();
      int var5 = var4.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         V v = var4[var6];
         this.versionedSerializers.put(v, new VersionSerializers(id.name(), (Version)v));
      }

   }

   public Verbs.Group id() {
      return this.id;
   }

   protected VerbGroup<V>.RegistrationHelper helper() {
      if(this.helperCreated) {
         throw new IllegalStateException("Should only create a single RegistrationHelper per group");
      } else {
         this.helperCreated = true;
         return new VerbGroup.RegistrationHelper();
      }
   }

   public boolean isInternal() {
      return this.isInternal;
   }

   VersionSerializers forVersion(V version) {
      return (VersionSerializers)this.versionedSerializers.get(version);
   }

   public Iterator<Verb<?, ?>> iterator() {
      return this.verbs.iterator();
   }

   public String toString() {
      return this.id.toString();
   }

   private static <T> ExecutorSupplier<T> maybeGetRequestExecutor(Class<T> requestClass, Stage defaultStage) {
      return SchedulableMessage.class.isAssignableFrom(requestClass)?(p) -> {
         return ((SchedulableMessage)p).getRequestExecutor();
      }:(defaultStage == null?null:(p) -> {
         return StageManager.getStage(defaultStage);
      });
   }

   private static <T> ExecutorSupplier<T> maybeGetResponseExecutor(Class<T> requestClass, boolean isInternal) {
      return SchedulableMessage.class.isAssignableFrom(requestClass)?(p) -> {
         return ((SchedulableMessage)p).getResponseExecutor();
      }:(p) -> {
         return StageManager.getStage(isInternal?Stage.INTERNAL_RESPONSE:Stage.REQUEST_RESPONSE);
      };
   }

   private static <T> Serializer<T> maybeGetSerializer(Class<T> klass) {
      try {
         Field field = klass.getField("serializer");
         return field.getType().equals(Serializer.class) && Modifier.isStatic(field.getModifiers())?(Serializer)field.get((Object)null):null;
      } catch (NoSuchFieldException var2) {
         return null;
      } catch (Exception var3) {
         logger.error("Error setting serializer for {}", klass, var3);
         return null;
      }
   }

   private <T> Function<V, Serializer<T>> maybeGetVersionedSerializers(Class<T> klass) {
      try {
         Field field = klass.getField("serializers");
         if(field.getType().equals(Versioned.class) && Modifier.isStatic(field.getModifiers())) {
            Versioned<V, Serializer<T>> versioned = (Versioned)field.get((Object)null);
            return (x$0) -> {
               return (Serializer)versioned.get((Enum)x$0);
            };
         } else {
            return null;
         }
      } catch (NoSuchFieldException var4) {
         return null;
      } catch (Exception var5) {
         logger.error("Error setting serializer for {}", klass, var5);
         return null;
      }
   }

   protected String getUnsupportedVersionMessage(MessagingVersion version) {
      return String.format("Group %s does not support messaging version %s.", new Object[]{this, version});
   }

   protected class RegistrationHelper {
      private int idx;
      private final int[] versionCodes;
      private Stage defaultStage;
      private DroppedMessages.Group defaultDroppedGroup;
      private boolean executeOnIOScheduler;

      protected RegistrationHelper() {
         this.versionCodes = new int[((Enum[])VerbGroup.this.versionClass.getEnumConstants()).length];
      }

      public VerbGroup<V>.RegistrationHelper stage(Stage defaultStage) {
         this.defaultStage = defaultStage;
         return this;
      }

      public VerbGroup<V>.RegistrationHelper droppedGroup(DroppedMessages.Group defaultDroppedGroup) {
         this.defaultDroppedGroup = defaultDroppedGroup;
         return this;
      }

      public <P> VerbGroup<V>.RegistrationHelper.OneWayBuilder<P> oneWay(String verbName, Class<P> klass) {
         return new VerbGroup.RegistrationHelper.OneWayBuilder(verbName, this.idx++, klass);
      }

      public <P> VerbGroup<V>.RegistrationHelper.AckedRequestBuilder<P> ackedRequest(String verbName, Class<P> klass) {
         return new VerbGroup.RegistrationHelper.AckedRequestBuilder(verbName, this.idx++, klass);
      }

      public <P, Q> VerbGroup<V>.RegistrationHelper.RequestResponseBuilder<P, Q> requestResponse(String verbName, Class<P> requestClass, Class<Q> responseClass) {
         return new VerbGroup.RegistrationHelper.RequestResponseBuilder(verbName, this.idx++, requestClass, responseClass, this.executeOnIOScheduler);
      }

      public <P extends Monitorable, Q> VerbGroup<V>.RegistrationHelper.MonitoredRequestResponseBuilder<P, Q> monitoredRequestResponse(String verbName, Class<P> requestClass, Class<Q> responseClass) {
         return new VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder(verbName, this.idx++, requestClass, responseClass);
      }

      public VerbGroup<V>.RegistrationHelper executeOnIOScheduler() {
         this.executeOnIOScheduler = true;
         return this;
      }

      public class MonitoredRequestResponseBuilder<P extends Monitorable, Q> extends VerbGroup<V>.RegistrationHelper.VerbBuilder<P, Q, VerbGroup<V>.RegistrationHelper.MonitoredRequestResponseBuilder<P, Q>> {
         private MonitoredRequestResponseBuilder(String this$1, int name, Class<P> groupIdx, Class<Q> requestClass) {
            super(name, groupIdx, false, requestClass, responseClass, null);
         }

         public Verb.RequestResponse<P, Q> handler(VerbHandlers.MonitoredRequestResponse<P, Q> handler) {
            return (Verb.RequestResponse)this.add(new Verb.RequestResponse(this.info(), this.timeoutSupplier(), handler));
         }

         public Verb.RequestResponse<P, Q> syncHandler(VerbHandlers.SyncMonitoredRequestResponse<P, Q> handler) {
            return (Verb.RequestResponse)this.add(new Verb.RequestResponse(this.info(), this.timeoutSupplier(), handler));
         }
      }

      public class RequestResponseBuilder<P, Q> extends VerbGroup<V>.RegistrationHelper.VerbBuilder<P, Q, VerbGroup<V>.RegistrationHelper.RequestResponseBuilder<P, Q>> {
         private RequestResponseBuilder(String this$1, int name, Class<P> groupIdx, Class<Q> requestClass, boolean responseClass) {
            super(name, groupIdx, false, requestClass, responseClass, null);
         }

         public Verb.RequestResponse<P, Q> handler(VerbHandlers.RequestResponse<P, Q> handler) {
            return (Verb.RequestResponse)this.add(new Verb.RequestResponse(this.info(), this.timeoutSupplier(), handler));
         }

         public Verb.RequestResponse<P, Q> syncHandler(VerbHandlers.SyncRequestResponse<P, Q> handler) {
            return (Verb.RequestResponse)this.add(new Verb.RequestResponse(this.info(), this.timeoutSupplier(), handler));
         }
      }

      public class AckedRequestBuilder<P> extends VerbGroup<V>.RegistrationHelper.VerbBuilder<P, EmptyPayload, VerbGroup<V>.RegistrationHelper.AckedRequestBuilder<P>> {
         private AckedRequestBuilder(String this$1, int name, Class<P> groupIdx) {
            super(name, groupIdx, false, requestClass, EmptyPayload.class, null);
         }

         public Verb.AckedRequest<P> handler(VerbHandlers.AckedRequest<P> handler) {
            return (Verb.AckedRequest)this.add(new Verb.AckedRequest(this.info(), this.timeoutSupplier(), handler));
         }

         public Verb.AckedRequest<P> syncHandler(VerbHandlers.SyncAckedRequest<P> handler) {
            return (Verb.AckedRequest)this.add(new Verb.AckedRequest(this.info(), this.timeoutSupplier(), handler));
         }
      }

      public class OneWayBuilder<P> extends VerbGroup<V>.RegistrationHelper.VerbBuilder<P, NoResponse, VerbGroup<V>.RegistrationHelper.OneWayBuilder<P>> {
         private OneWayBuilder(String this$1, int name, Class<P> groupIdx) {
            super(name, groupIdx, true, requestClass, NoResponse.class, null);
         }

         public Verb.OneWay<P> handler(VerbHandler<P, NoResponse> handler) {
            return (Verb.OneWay)this.add(new Verb.OneWay(this.info(), handler));
         }

         public Verb.OneWay<P> handler(VerbHandlers.OneWay<P> handler) {
            return (Verb.OneWay)this.add(new Verb.OneWay(this.info(), handler));
         }
      }

      public class VerbBuilder<P, Q, T> {
         private final String name;
         private final int groupIdx;
         private final boolean isOneWay;
         private TimeoutSupplier<P> timeoutSupplier;
         private ExecutorSupplier<P> requestExecutor;
         private ExecutorSupplier<P> responseExecutor;
         private Serializer<P> requestSerializer;
         private Serializer<Q> responseSerializer;
         private Function<V, Serializer<P>> requestSerializerFct;
         private Function<V, Serializer<Q>> responseSerializerFct;
         private DroppedMessages.Group droppedGroup;
         private ErrorHandler errorHandler;
         private V sinceVersion;
         private V untilVersion;
         private boolean supportsBackPressure;

         private VerbBuilder(String this$1, int name, boolean groupIdx, Class<P> isOneWay, Class<Q> requestClass) {
            this.errorHandler = ErrorHandler.DEFAULT;
            this.name = name;
            this.groupIdx = groupIdx;
            this.isOneWay = isOneWay;
            this.requestExecutor = VerbGroup.maybeGetRequestExecutor(requestClass, RegistrationHelper.this.defaultStage);
            this.responseExecutor = VerbGroup.maybeGetResponseExecutor(requestClass, VerbGroup.this.isInternal);
            this.requestSerializer = VerbGroup.maybeGetSerializer(requestClass);
            this.responseSerializer = VerbGroup.maybeGetSerializer(responseClass);
            this.requestSerializerFct = VerbGroup.this.maybeGetVersionedSerializers(requestClass);
            this.responseSerializerFct = VerbGroup.this.maybeGetVersionedSerializers(responseClass);
            this.droppedGroup = RegistrationHelper.this.defaultDroppedGroup;
         }

         private T us() {
            return this;
         }

         public T timeout(TimeoutSupplier<P> supplier) {
            this.timeoutSupplier = supplier;
            return this.us();
         }

         public T timeout(Supplier<Long> supplier) {
            this.timeoutSupplier = (request) -> {
               return ((Long)supplier.get()).longValue();
            };
            return this.us();
         }

         public T timeout(int timeout, TimeUnit unit) {
            long timeoutMillis = unit.toMillis((long)timeout);
            return this.timeout((request) -> {
               return timeoutMillis;
            });
         }

         public T requestStage(Stage stage) {
            this.requestExecutor = (p) -> {
               return StageManager.getStage(stage);
            };
            return this.us();
         }

         public T requestExecutor(TracingAwareExecutor tae) {
            this.requestExecutor = (p) -> {
               return tae;
            };
            return this.us();
         }

         public T responseExecutor(ExecutorSupplier<P> responseExecutor) {
            this.responseExecutor = responseExecutor;
            return this.us();
         }

         public T droppedGroup(DroppedMessages.Group droppedGroup) {
            this.droppedGroup = droppedGroup;
            return this.us();
         }

         public T withRequestSerializer(Serializer<P> serializer) {
            this.requestSerializer = serializer;
            return this.us();
         }

         public T withRequestSerializer(Function<V, Serializer<P>> serializerFunction) {
            this.requestSerializerFct = serializerFunction;
            return this.us();
         }

         public T withResponseSerializer(Serializer<Q> serializer) {
            assert !this.isOneWay : "Shouldn't set the response serializer of one-way verbs";

            this.responseSerializer = serializer;
            return this.us();
         }

         public T withResponseSerializer(Function<V, Serializer<Q>> serializerFunction) {
            assert !this.isOneWay : "Shouldn't set the response serializer of one-way verbs";

            this.responseSerializerFct = serializerFunction;
            return this.us();
         }

         public T withBackPressure() {
            this.supportsBackPressure = true;
            return this.us();
         }

         public T withErrorHandler(ErrorHandler handler) {
            this.errorHandler = handler;
            return this.us();
         }

         public T since(V version) {
            if(this.sinceVersion != null) {
               throw new IllegalStateException("since() should be called at most once for each verb");
            } else {
               this.sinceVersion = version;
               return this.us();
            }
         }

         public T until(V version) {
            if(this.untilVersion != null) {
               throw new IllegalStateException("until() should be called at most once for each verb");
            } else {
               this.untilVersion = version;
               return this.us();
            }
         }

         protected Verb.Info<P> info() {
            if(this.requestExecutor == null) {
               throw new IllegalStateException("Unless the request payload implements the SchedulableMessage interface, a request stage is required (either at the RegistrationHelper level or at the VerbBuilder one)");
            } else if(this.isOneWay && this.supportsBackPressure) {
               throw new IllegalStateException("Back pressure doesn't make sense for one-way message (no response is sent so we can't keep track of in-flight requests to an host)");
            } else if(!this.isOneWay && this.droppedGroup == null) {
               throw new IllegalStateException("Missing 'dropped group', should be indicated either at the RegistrationHelper lever or at the VerbBuilder one");
            } else {
               return new Verb.Info(VerbGroup.this, this.groupIdx, this.name, this.requestExecutor, this.responseExecutor, this.supportsBackPressure, this.isOneWay?null:this.droppedGroup, this.errorHandler);
            }
         }

         TimeoutSupplier<P> timeoutSupplier() {
            if(this.isOneWay && this.timeoutSupplier != null) {
               throw new IllegalStateException("One way verb should not define a timeout supplier, we'll never timeout them");
            } else if(!this.isOneWay && this.timeoutSupplier == null) {
               throw new IllegalStateException("Non-one way verb must define a timeout supplier");
            } else {
               return this.timeoutSupplier;
            }
         }

         <X extends Verb<P, Q>> X add(X verb) {
            VerbGroup.this.verbs.add(verb);
            Enum[] var2 = (Enum[])VerbGroup.this.versionClass.getEnumConstants();
            int var3 = var2.length;

            for(int var4 = 0; var4 < var3; ++var4) {
               V v = var2[var4];
               if(this.sinceVersion == null || v.compareTo(this.sinceVersion) >= 0) {
                  if(this.untilVersion != null && v.compareTo(this.untilVersion) > 0) {
                     break;
                  }

                  int code = RegistrationHelper.this.versionCodes[v.ordinal()]++;
                  Serializer<P> reqSerializer = this.requestSerializerFct == null?this.requestSerializer:(Serializer)this.requestSerializerFct.apply(v);
                  if(reqSerializer == null) {
                     throw new IllegalStateException(String.format("No request serializer defined for verb %s and no default one found.", new Object[]{this.name}));
                  }

                  Serializer<Q> respSerializer = null;
                  if(!this.isOneWay) {
                     respSerializer = this.responseSerializerFct == null?this.responseSerializer:(Serializer)this.responseSerializerFct.apply(v);
                     if(respSerializer == null) {
                        throw new IllegalStateException(String.format("No response serializer defined for verb %s and no default one found.", new Object[]{this.name}));
                     }
                  }

                  ((VersionSerializers)VerbGroup.this.versionedSerializers.get(v)).add(verb, code, reqSerializer, respSerializer);
               }
            }

            return verb;
         }
      }
   }
}
