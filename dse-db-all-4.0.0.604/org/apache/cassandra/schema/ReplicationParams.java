package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

public final class ReplicationParams {
   public static final String CLASS = "class";
   public final Class<? extends AbstractReplicationStrategy> klass;
   public final ImmutableMap<String, String> options;

   private ReplicationParams(Class<? extends AbstractReplicationStrategy> klass, Map<String, String> options) {
      this.klass = klass;
      this.options = ImmutableMap.copyOf(options);
   }

   static ReplicationParams local() {
      return new ReplicationParams(LocalStrategy.class, ImmutableMap.of());
   }

   static ReplicationParams simple(int replicationFactor) {
      return new ReplicationParams(SimpleStrategy.class, ImmutableMap.of("replication_factor", Integer.toString(replicationFactor)));
   }

   static ReplicationParams nts(Object... args) {
      assert args.length % 2 == 0;

      Map<String, String> options = new HashMap();

      for(int i = 0; i < args.length; i += 2) {
         String dc = (String)args[i];
         Integer rf = (Integer)args[i + 1];
         options.put(dc, rf.toString());
      }

      return new ReplicationParams(NetworkTopologyStrategy.class, options);
   }

   public void validate(String name) {
      TokenMetadata tmd = StorageService.instance.getTokenMetadata();
      IEndpointSnitch eps = DatabaseDescriptor.getEndpointSnitch();
      AbstractReplicationStrategy.validateReplicationStrategy(name, this.klass, tmd, eps, this.options);
   }

   public static ReplicationParams fromMap(Map<String, String> map) {
      Map<String, String> options = new HashMap(map);
      String className = (String)options.remove("class");
      Class<? extends AbstractReplicationStrategy> klass = AbstractReplicationStrategy.getClass(className);
      return new ReplicationParams(klass, options);
   }

   public Map<String, String> asMap() {
      Map<String, String> map = new HashMap(this.options);
      map.put("class", this.klass.getName());
      return map;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ReplicationParams)) {
         return false;
      } else {
         ReplicationParams r = (ReplicationParams)o;
         return this.klass.equals(r.klass) && this.options.equals(r.options);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.klass, this.options});
   }

   public String toString() {
      ToStringHelper helper = MoreObjects.toStringHelper(this);
      helper.add("class", this.klass.getName());
      UnmodifiableIterator var2 = this.options.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<String, String> entry = (Entry)var2.next();
         helper.add((String)entry.getKey(), entry.getValue());
      }

      return helper.toString();
   }
}
