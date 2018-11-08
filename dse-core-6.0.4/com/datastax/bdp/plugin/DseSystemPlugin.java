package com.datastax.bdp.plugin;

import com.datastax.bdp.cassandra.auth.DseResourceFactory;
import com.datastax.bdp.system.DseLocalKeyspace;
import com.datastax.bdp.system.DseSystemKeyspace;
import com.datastax.bdp.system.SolrKeyspace;
import com.google.inject.Singleton;
import org.apache.cassandra.auth.Resources;

@Singleton
@DsePlugin(
   dependsOn = {}
)
public class DseSystemPlugin extends AbstractPlugin {
   public DseSystemPlugin() {
   }

   public void setupSchema() {
      Resources.setResourceFactory(new DseResourceFactory());
      DseSystemKeyspace.maybeConfigure();
      DseLocalKeyspace.maybeConfigure();
      SolrKeyspace.maybeConfigure();
   }

   public boolean isEnabled() {
      return true;
   }
}
