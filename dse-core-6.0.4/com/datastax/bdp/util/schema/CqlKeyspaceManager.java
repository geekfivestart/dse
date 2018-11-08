package com.datastax.bdp.util.schema;

import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.util.DseUtil;
import com.datastax.bdp.util.LazyRef;
import com.datastax.bdp.util.SchemaTool;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.validation.ValidationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EverywhereStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CqlKeyspaceManager extends CqlAbstractManager {
   public static final Logger logger = LoggerFactory.getLogger(CqlKeyspaceManager.class);
   private LazyRef<String> keyspaceName = LazyRef.of(() -> {
      return this.getKeyspaceMetadata().name;
   });

   public CqlKeyspaceManager() {
   }

   protected abstract KeyspaceMetadata getKeyspaceMetadata();

   public String getName() {
      return (String)this.keyspaceName.get();
   }

   public CqlKeyspaceManager.KeyspaceValidationReport validate(KeyspaceMetadata metadata) {
      Map<String, InconsistentValue<Object>> paramsReport = this.reportDifferentParams(metadata);
      CqlKeyspaceManager.KeyspaceValidationReport report = new CqlKeyspaceManager.KeyspaceValidationReport((String)this.keyspaceName.get(), true, paramsReport);
      return report;
   }

   private Optional<KeyspaceMetadata> getMetaData(String ksName) {
      return SchemaTool.cql3KeyspaceExists(ksName)?Optional.ofNullable(SchemaTool.getKeyspaceMetadata(ksName)):Optional.empty();
   }

   public void create() throws IOException, ValidationException {
      Optional<KeyspaceMetadata> ksDef = (Optional)this.getMetaData((String)this.keyspaceName.get()).map(Optional::of).orElseGet(() -> {
         this.waitAndMaybeCreate();
         return this.getMetaData((String)this.keyspaceName.get());
      });
      if(!ksDef.isPresent()) {
         throw new IOException(String.format("Failed to create keyspace %s", new Object[]{this.keyspaceName.get()}));
      } else {
         logger.info("Validating keyspace {}", this.keyspaceName.get());
         CqlKeyspaceManager.KeyspaceValidationReport report = this.validate((KeyspaceMetadata)ksDef.get());
         if(!report.valid) {
            throw new ValidationException(String.format("Keyspace %s is corrupted. Validation report: \n%s", new Object[]{this.keyspaceName.get(), report.toString()}));
         }
      }
   }

   public void verifyReplicationConfig() {
      Optional<KeyspaceMetadata> ksMetadata = this.getMetaData((String)this.keyspaceName.get());
      if(!ksMetadata.isPresent()) {
         logger.error("keyspace {} doesn't exists", this.keyspaceName.get());
      } else {
         Map<String, Long> datacenters = EndpointStateTracker.instance.getAllKnownDatacenters();
         ReplicationParams replParams = ((KeyspaceMetadata)ksMetadata.get()).params.replication;
         Class<? extends AbstractReplicationStrategy> replicationClass = replParams.klass;
         String localDC = DseUtil.getDatacenter();
         Map<String, String> replication = replParams.asMap();
         if(replicationClass.equals(SimpleStrategy.class)) {
            String replicationFactor = this.getRF(localDC, replication);
            if(datacenters.size() > 1) {
               logger.warn("{} is not recommended, please use {} with a RF of 3 or the number of nodes in the datacenter, whichever is smaller", SimpleStrategy.class.getCanonicalName(), NetworkTopologyStrategy.class.getCanonicalName());
            } else if(replicationFactor == null) {
               logger.warn("keyspace {} replication factor is null", this.getName());
            } else {
               Set<Workload> workloads = (Set)EndpointStateTracker.instance.getDatacenterWorkloads().get(localDC);
               if(workloads != null && workloads.contains(Workload.Analytics)) {
                  this.checkRF(localDC, datacenters, replication);
               }
            }
         } else {
            Map datacenterWorkloads;
            if(replicationClass.equals(NetworkTopologyStrategy.class)) {
               datacenterWorkloads = EndpointStateTracker.instance.getDatacenterWorkloads();
               datacenterWorkloads.entrySet().stream().filter((entry) -> {
                  return ((Set)entry.getValue()).contains(Workload.Analytics);
               }).forEach((entry) -> {
                  this.checkRF((String)entry.getKey(), datacenters, replication);
               });
            } else if(replicationClass.equals(EverywhereStrategy.class)) {
               datacenterWorkloads = EndpointStateTracker.instance.getDatacenterWorkloads();
               datacenterWorkloads.entrySet().stream().filter((entry) -> {
                  return ((Set)entry.getValue()).contains(Workload.Analytics) && ((Long)datacenters.getOrDefault(entry.getKey(), Long.valueOf(1L))).longValue() > 10L;
               }).forEach((entry) -> {
                  logger.warn("If there are timeouts when reading or writing to {} at datacenter {}, the admin should consider using NTS with fixed RF", this.getName(), entry.getKey());
               });
            }
         }
      }

   }

   private void checkRF(String dc, Map<String, Long> datacenters, Map<String, String> replication) {
      int minRf = Math.min(3, ((Long)datacenters.getOrDefault(dc, Long.valueOf(1L))).intValue());
      int rf = Integer.valueOf(this.getRF(dc, replication)).intValue();
      if(rf < minRf) {
         logger.warn("Keyspace {} replication factor for datacenter {} is {}. Please change it to at least {} for better stability.", new Object[]{this.getName(), dc, Integer.valueOf(rf), Integer.valueOf(minRf)});
      }

   }

   private String getRF(String dc, Map<String, String> replication) {
      String rf = (String)replication.get(dc);
      return rf == null?(String)replication.get("replication_factor"):rf;
   }

   protected Map<String, InconsistentValue<Object>> reportDifferentParams(KeyspaceMetadata currentMetaData) {
      KeyspaceParams curParams = currentMetaData.params;
      KeyspaceParams expParams = this.getKeyspaceMetadata().params;
      Map<String, InconsistentValue<Object>> inconsistencies = new HashMap();
      this.maybeReport(inconsistencies, "durable_writes", Boolean.valueOf(curParams.durableWrites), Boolean.valueOf(expParams.durableWrites));
      this.maybeReport(inconsistencies, "replication", curParams.replication, expParams.replication);
      return inconsistencies;
   }

   private boolean waitAndMaybeCreate() {
      logger.info("Creating keyspace {}", this.keyspaceName.get());

      try {
         SchemaTool.waitForRingToStabilize((String)this.keyspaceName.get());
         SchemaTool.maybeCreateOrUpdateKeyspace(this.getKeyspaceMetadata());
         return true;
      } catch (Exception var2) {
         logger.warn("Could not create keyspace {}. The error message was: {}", this.keyspaceName.get(), var2.getMessage());
         return false;
      }
   }

   public static class KeyspaceValidationReport {
      public final String keyspace;
      public final boolean valid;
      public final Map<String, InconsistentValue<Object>> inconsistentParams;

      public KeyspaceValidationReport(String keyspace, boolean valid, Map<String, InconsistentValue<Object>> inconsistentParams) {
         this.keyspace = keyspace;
         this.valid = valid;
         this.inconsistentParams = Collections.unmodifiableMap(inconsistentParams);
      }

      public String toString() {
         StringBuilder report = (new StringBuilder("Validation report of keyspace ")).append(this.keyspace).append(": ");
         if(this.valid) {
            report.append("OK");
         } else {
            report.append("Invalid");
         }

         report.append("\n");
         report.append((String)this.inconsistentParams.entrySet().stream().map((i) -> {
            return String.format(" - %s: %s", new Object[]{i.getKey(), i.getValue()});
         }).reduce((a, b) -> {
            return a + "\n" + b;
         }).orElse(""));
         return report.toString();
      }
   }
}
