package org.apache.cassandra.cql3.statements;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;

public final class KeyspaceAttributes extends PropertyDefinitions {
   private static final Set<String> validKeywords;
   private static final Set<String> obsoleteKeywords;

   public KeyspaceAttributes() {
   }

   public void validate() {
      this.validate(validKeywords, obsoleteKeywords);
      Map<String, String> replicationOptions = this.getAllReplicationOptions();
      if(!replicationOptions.isEmpty() && !replicationOptions.containsKey("class")) {
         throw new ConfigurationException("Missing replication strategy class");
      }
   }

   public String getReplicationStrategyClass() {
      return (String)this.getAllReplicationOptions().get("class");
   }

   public Map<String, String> getReplicationOptions() {
      Map<String, String> replication = new HashMap(this.getAllReplicationOptions());
      replication.remove("class");
      return replication;
   }

   public Map<String, String> getAllReplicationOptions() {
      Map<String, String> replication = this.getMap(KeyspaceParams.Option.REPLICATION.toString());
      return replication == null?Collections.emptyMap():replication;
   }

   public KeyspaceParams asNewKeyspaceParams() {
      boolean durableWrites = this.getBoolean(KeyspaceParams.Option.DURABLE_WRITES.toString(), Boolean.valueOf(true)).booleanValue();
      return KeyspaceParams.create(durableWrites, this.getAllReplicationOptions());
   }

   public KeyspaceParams asAlteredKeyspaceParams(KeyspaceParams previous) {
      boolean durableWrites = this.getBoolean(KeyspaceParams.Option.DURABLE_WRITES.toString(), Boolean.valueOf(previous.durableWrites)).booleanValue();
      ReplicationParams replication = this.getReplicationStrategyClass() == null?previous.replication:ReplicationParams.fromMap(this.getAllReplicationOptions());
      return new KeyspaceParams(durableWrites, replication);
   }

   public boolean hasOption(KeyspaceParams.Option option) {
      return this.hasProperty(option.toString()).booleanValue();
   }

   static {
      Builder<String> validBuilder = ImmutableSet.builder();
      KeyspaceParams.Option[] var1 = KeyspaceParams.Option.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         KeyspaceParams.Option option = var1[var3];
         validBuilder.add(option.toString());
      }

      validKeywords = validBuilder.build();
      obsoleteKeywords = ImmutableSet.of();
   }
}
