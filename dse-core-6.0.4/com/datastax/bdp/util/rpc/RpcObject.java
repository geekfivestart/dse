package com.datastax.bdp.util.rpc;

import com.datastax.bdp.ioc.DseInjector;
import com.datastax.bdp.router.InternalQueryRouter;
import com.datastax.bdp.util.LazyRef;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class RpcObject {
   protected final String name;
   protected final Object raw;
   protected final ImmutableMap<String, RpcMethod> rpcs;
   protected final LazyRef<InternalQueryRouter> internalQueryRouter = LazyRef.of(() -> {
      return (InternalQueryRouter)DseInjector.get().getInstance(InternalQueryRouter.class);
   });

   protected RpcObject(String name, Object object) {
      this.name = name;
      this.raw = object;
      Map<String, RpcMethod> found = new HashMap();
      Method[] var4 = object.getClass().getMethods();
      int var5 = var4.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         Method method = var4[var6];
         if(method.isAnnotationPresent(Rpc.class)) {
            RpcMethod rpcMethod = new RpcMethod(method, this);
            if(found.containsKey(rpcMethod.getName())) {
               throw new AssertionError(String.format("Naming conflict in class %s: method %s already exists. Cannot have two @Rpc annotated methods with the same name.", new Object[]{object.getClass().getCanonicalName(), rpcMethod.getName()}));
            }

            found.put(rpcMethod.getName(), rpcMethod);
         }
      }

      this.rpcs = ImmutableMap.copyOf(found);
   }

   protected Optional<InetAddress> getEndpoint(String methodName, LazyRef<Object[]> args) {
      return this.raw instanceof RoutableRpcSupport?Optional.ofNullable(((RoutableRpcSupport)this.raw).getEndpoint(methodName, args)):Optional.empty();
   }

   protected String getName() {
      return this.name;
   }

   protected Optional<RpcMethod> lookupMethod(String name) {
      return Optional.ofNullable(this.rpcs.get(name));
   }

   protected Collection<RpcMethod> getMethods() {
      return this.rpcs.values();
   }

   protected RpcMethod getMethod(String name) {
      return this.rpcs.containsKey(name)?(RpcMethod)this.rpcs.get(name):null;
   }
}
