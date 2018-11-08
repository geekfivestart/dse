package com.datastax.bdp.util.rpc;

import com.datastax.bdp.cassandra.auth.RpcResource;
import com.datastax.bdp.cassandra.cql3.RpcCallStatement;
import com.datastax.bdp.router.InternalQueryRouter;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.LazyRef;
import com.datastax.bdp.util.genericql.GenericSerializer;
import com.datastax.bdp.util.genericql.ObjectSerializer;
import com.google.common.base.Preconditions;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.concurrent.TPCUtils.WouldBlockException;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.transport.messages.ResultMessage.Void;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcMethod {
   private static final Logger logger = LoggerFactory.getLogger(RpcMethod.class);
   private final Method method;
   private final RpcObject rpcObject;
   private final String name;
   private final Permission permission;
   private final List<TypeSerializer> argSerializers;
   private final List<AbstractType> argTypes;
   private final List<String> argNames;
   private final ObjectSerializer retSerializer;
   private final OptionalInt clientStateArgIdx;
   private final List<Pair<Integer, RpcParam>> params;

   <R> RpcMethod(Method method, RpcObject rpcObject) {
      this.method = method;
      this.rpcObject = rpcObject;
      this.name = ((Rpc)method.getAnnotation(Rpc.class)).name();
      this.permission = ((Rpc)method.getAnnotation(Rpc.class)).permission();
      Annotation[][] allAnnotations = method.getParameterAnnotations();
      this.params = (List)IntStream.range(0, method.getParameterCount()).boxed().flatMap((argIdx) -> {
         Optional var10000 = Arrays.stream(allAnnotations[argIdx.intValue()]).filter((a) -> {
            return a instanceof RpcParam;
         }).findFirst();
         RpcParam.class.getClass();
         return (Stream)var10000.map(RpcParam.class::cast).map((rpcParam) -> {
            return Stream.of(Pair.of(argIdx, rpcParam));
         }).orElseGet(Stream::empty);
      }).collect(Collectors.toList());
      Class<?>[] paramTypes = method.getParameterTypes();
      this.clientStateArgIdx = IntStream.range(0, method.getParameterCount()).filter((argIdx) -> {
         return paramTypes[argIdx] == RpcClientState.class;
      }).findFirst();
      int expectedParamsCount = this.params.size() + (this.clientStateArgIdx.isPresent()?1:0);
      if(method.getParameterCount() != expectedParamsCount) {
         throw new AssertionError(String.format("All arguments for %s.%s must be annotated with either RpcParam or RpcClientState", new Object[]{rpcObject.getName(), this.name}));
      } else {
         Type[] genericParamTypes = method.getGenericParameterTypes();
         this.argSerializers = (List)this.params.stream().map((p) -> {
            return GenericSerializer.getSerializer(genericParamTypes[((Integer)p.getKey()).intValue()]);
         }).collect(Collectors.toList());
         this.argTypes = (List)this.params.stream().map((p) -> {
            return GenericSerializer.getTypeOrException(genericParamTypes[((Integer)p.getKey()).intValue()]);
         }).collect(Collectors.toList());
         this.argNames = (List)this.params.stream().map((p) -> {
            return ((RpcParam)p.getValue()).name();
         }).collect(Collectors.toList());
         if(((Rpc)method.getAnnotation(Rpc.class)).multiRow()) {
            Preconditions.checkArgument(Collection.class.isAssignableFrom(method.getReturnType()), "If mutli-row result set is requested, the method return type must be an implementation of java.util.Collection");
            Type elemType = ((ParameterizedType)method.getGenericReturnType()).getActualTypeArguments()[0];
            Preconditions.checkArgument(elemType instanceof Class, "If multi-row result set is request, the element type must be a Class");
            this.retSerializer = new ObjectSerializer((Class)elemType);
         } else {
            this.retSerializer = new ObjectSerializer(method.getReturnType(), method.getGenericReturnType());
         }

      }
   }

   public String getName() {
      return this.name;
   }

   public Permission getPermission() {
      return this.permission;
   }

   public int getArgumentCount() {
      return this.argTypes.size();
   }

   public ColumnSpecification getArgumentSpecification(int position) {
      return new ColumnSpecification("system", this.rpcObject.getName() + "." + this.name, new ColumnIdentifier((String)this.argNames.get(position), false), (AbstractType)this.argTypes.get(position));
   }

   public void checkAccess(QueryState state) throws UnauthorizedException {
      state.checkPermission(RpcResource.method(this.rpcObject.getName(), this.name), this.permission);
   }

   public ResultMessage execute(ClientState clientState, List<ByteBuffer> parameters) throws RequestExecutionException {
      try {
         RpcClientState rpcClientState = RpcClientState.fromClientState(clientState);
         LazyRef<Object[]> rpcArgs = LazyRef.of(() -> {
            return this.getMethodArgs(rpcClientState, parameters);
         });
         Optional<InetAddress> endpoint = this.rpcObject.getEndpoint(this.method.getName(), rpcArgs);
         if(endpoint.isPresent() && !Addresses.Internode.isLocalEndpoint((InetAddress)endpoint.get())) {
            Future<ResultMessage> result = ((InternalQueryRouter)this.rpcObject.internalQueryRouter.get()).executeRpcRemote((InetAddress)endpoint.get(), this.rpcObject.name, this.name, clientState, parameters);
            return InternalQueryRouter.withQueryExceptionsHandled(() -> {
               return (ResultMessage)result.get(RpcCallStatement.RPC_ROUTED_CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            });
         } else {
            return this.toResultMessage(this.method.invoke(this.rpcObject.raw, (Object[])rpcArgs.get()));
         }
      } catch (WouldBlockException var7) {
         throw var7;
      } catch (InvocationTargetException var8) {
         if(var8.getCause() instanceof WouldBlockException) {
            throw (WouldBlockException)var8.getCause();
         } else {
            throw this.createRpcExecutionException(var8);
         }
      } catch (Exception var9) {
         throw this.createRpcExecutionException(var9);
      }
   }

   private RpcExecutionException createRpcExecutionException(Throwable e) {
      String msg = String.format("Failed to execute method %s.%s", new Object[]{this.rpcObject.getName(), this.name});
      logger.info(msg, e);
      return RpcExecutionException.create(msg, e);
   }

   private Object[] getMethodArgs(RpcClientState rpcClientState, Collection<ByteBuffer> parameters) {
      Object[] args = new Object[this.method.getParameterCount()];
      this.clientStateArgIdx.ifPresent((idx) -> {
         args[idx] = rpcClientState;
      });
      Object[] rpcParams = this.deserializeParameters(parameters);

      for(int i = 0; i < rpcParams.length; ++i) {
         args[((Integer)((Pair)this.params.get(i)).getKey()).intValue()] = rpcParams[i];
      }

      return args;
   }

   public ResultSet toResultSet(Object object) {
      return ((Rpc)this.method.getAnnotation(Rpc.class)).multiRow()?this.retSerializer.toMultiRowResultSet((Collection)object, this.rpcObject.getName(), this.name):this.retSerializer.toResultSet(object, this.rpcObject.getName(), this.name);
   }

   public ResultMessage toResultMessage(Object object) {
      return (ResultMessage)(object == null?new Void():new Rows(this.toResultSet(object)));
   }

   private Object[] deserializeParameters(Collection<ByteBuffer> args) {
      Object[] deserialized = new Object[args.size()];
      int i = 0;

      for(Iterator var4 = args.iterator(); var4.hasNext(); ++i) {
         ByteBuffer arg = (ByteBuffer)var4.next();
         deserialized[i] = arg != null?((TypeSerializer)this.argSerializers.get(i)).deserialize(arg):null;
      }

      return deserialized;
   }
}
