package com.datastax.bdp;

import com.datastax.bdp.server.AbstractDseModule;
import com.google.inject.AbstractModule;

public class DseClientModule extends AbstractDseModule {
   public DseClientModule() {
   }

   protected void configure() {
      super.configure();
   }

   public String[] getOptionalModuleNames() {
      return new String[]{"com.datastax.bdp.DseSearchClientModule", "com.datastax.bdp.DseGraphClientModule", "com.datastax.bdp.DseAnalyticsClientModule"};
   }

   public AbstractModule[] getRequiredModules() {
      return new AbstractModule[]{new DseCoreClientModule()};
   }
}
