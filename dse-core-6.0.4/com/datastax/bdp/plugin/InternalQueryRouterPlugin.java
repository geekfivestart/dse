package com.datastax.bdp.plugin;

import com.datastax.bdp.node.transport.internode.InternodeClient;
import com.datastax.bdp.node.transport.internode.InternodeProtocolRegistry;
import com.datastax.bdp.router.InternalQueryRouter;
import com.datastax.bdp.router.InternalQueryRouterProtocol;
import com.datastax.bdp.server.LifecycleAware;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

@Singleton
@DsePlugin(
   dependsOn = {}
)
public class InternalQueryRouterPlugin extends AbstractPlugin implements Provider<InternalQueryRouter>, LifecycleAware {
   private final InternodeProtocolRegistry protocolRegistry;
   private final InternalQueryRouterProtocol protocol;
   private final InternalQueryRouter queryRouter;

   @Inject
   public InternalQueryRouterPlugin(InternodeProtocolRegistry protocolRegistry, InternalQueryRouterProtocol protocol, InternodeClient client) {
      this.protocolRegistry = protocolRegistry;
      this.protocol = protocol;
      this.queryRouter = new InternalQueryRouter(client);
   }

   public void preSetup() {
      this.protocolRegistry.register(this.protocol);
   }

   public InternalQueryRouter get() {
      return this.queryRouter;
   }

   public boolean isEnabled() {
      return true;
   }
}
