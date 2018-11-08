package com.datastax.bdp.util.schema;

import com.datastax.bdp.util.LazyRef;
import com.datastax.bdp.util.SchemaTool;
import com.diffplug.common.base.Errors;
import com.diffplug.common.base.Throwing.Supplier;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.validation.ValidationException;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.service.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CqlTableManager extends CqlAbstractManager {
   private static final Logger logger = LoggerFactory.getLogger(CqlTableManager.class);
   protected LazyRef<CreateTableStatement> createTableStmt = LazyRef.of(Errors.rethrow().wrap(() -> {
      CQLStatement stmt = QueryProcessor.parseStatement(this.getCreateTableCql(), QueryState.forInternalCalls()).statement;
      if(stmt instanceof CreateTableStatement) {
         return (CreateTableStatement)stmt;
      } else {
         throw new IllegalArgumentException(String.format("Not a CREATE TABLE statement %s", new Object[]{stmt}));
      }
   }));
   private LazyRef<String> keyspaceName = LazyRef.of(() -> {
      return ((CreateTableStatement)this.createTableStmt.get()).toTableMetadata().keyspace;
   });
   private LazyRef<String> tableName = LazyRef.of(() -> {
      return ((CreateTableStatement)this.createTableStmt.get()).toTableMetadata().name;
   });

   public CqlTableManager() {
   }

   protected abstract String getCreateTableCql();

   public String getName() {
      return (String)this.tableName.get();
   }

   public String getKeyspace() {
      return (String)this.keyspaceName.get();
   }

   protected CqlTableManager.TableValidationReport validate(TableMetadata metadata) {
      Map<String, InconsistentValue<Object>> paramsReport = this.reportDifferentParams(metadata);
      Set<ColumnMetadata> missingColsReport = this.reportMissingColumns(metadata);
      Set<ColumnMetadata> unexpectedColsReport = this.reportUnexpectedColumns(metadata);
      boolean valid = this.isCompatible(paramsReport, missingColsReport, unexpectedColsReport).booleanValue();
      CqlTableManager.TableValidationReport report = new CqlTableManager.TableValidationReport((String)this.keyspaceName.get(), (String)this.tableName.get(), valid, paramsReport, missingColsReport, unexpectedColsReport);
      return report;
   }

   private Optional<TableMetadata> getMetaData(String ksName, String cfName) {
      return SchemaTool.cql3TableExists(ksName, cfName)?Optional.of(SchemaTool.getTableMetadata(ksName, cfName)):Optional.empty();
   }

   public void create(Boolean force) throws IOException {
      if(!SchemaTool.cql3KeyspaceExists((String)this.keyspaceName.get())) {
         throw new IOException(String.format("Cannot find %s keyspace.", new Object[]{this.keyspaceName.get()}));
      } else {
         Optional<TableMetadata> cfDef = (Optional)this.getMetaData((String)this.keyspaceName.get(), (String)this.tableName.get()).map(Optional::of).orElseGet(() -> {
            this.waitAndMaybeCreate();
            return this.getMetaData((String)this.keyspaceName.get(), (String)this.tableName.get());
         });
         if(!cfDef.isPresent()) {
            throw new IOException(String.format("Table %s.%s setup failed.", new Object[]{this.keyspaceName.get(), this.tableName.get()}));
         } else {
            logger.info("Validating table {}.{}", this.keyspaceName.get(), this.tableName.get());
            CqlTableManager.TableValidationReport report = this.validate((TableMetadata)cfDef.get());
            if(!report.valid) {
               String validationMessage = String.format("Table %s.%s is corrupted. Validation report: \n%s", new Object[]{this.keyspaceName.get(), this.tableName.get(), report});
               if(!force.booleanValue()) {
                  throw new ValidationException(validationMessage);
               }

               logger.info(validationMessage);
               this.drop();
               this.create(Boolean.valueOf(false));
            }

         }
      }
   }

   private Boolean drop() {
      try {
         SchemaTool.waitForRingToStabilize((String)this.keyspaceName.get());
         SchemaTool.maybeDropTable((String)this.keyspaceName.get(), (String)this.tableName.get());
         return Boolean.valueOf(true);
      } catch (Exception var2) {
         logger.warn("Could not remove table {}.{}. The exception was: {}", new Object[]{this.keyspaceName, this.tableName, var2.getMessage()});
         return Boolean.valueOf(false);
      }
   }

   private Boolean waitAndMaybeCreate() {
      logger.info("Creating table {}.{}", this.keyspaceName, this.tableName);
      String stmt = this.getCreateTableCql();
      this.createTableStmt.get();

      try {
         SchemaTool.waitForRingToStabilize((String)this.keyspaceName.get());
         SchemaTool.maybeCreateTable((String)this.keyspaceName.get(), (String)this.tableName.get(), stmt);
         return Boolean.valueOf(true);
      } catch (Exception var3) {
         logger.warn("Could not create table {}.{}. The error message was: {}", new Object[]{this.keyspaceName, this.tableName, var3.getMessage()});
         return Boolean.valueOf(false);
      }
   }

   protected Boolean isCompatible(Map<String, InconsistentValue<Object>> inconsistentParams, Set<ColumnMetadata> missingCols, Set<ColumnMetadata> unexpectedCols) {
      boolean unexpectedPrimaryKeyCols = unexpectedCols.stream().map((c) -> {
         return c.kind;
      }).anyMatch((k) -> {
         return k == Kind.CLUSTERING || k == Kind.PARTITION_KEY;
      });
      return Boolean.valueOf(missingCols.isEmpty() && !unexpectedPrimaryKeyCols);
   }

   protected Set<ColumnMetadata> reportMissingColumns(TableMetadata curMetaData) {
      Set<ColumnMetadata> curCols = Sets.newHashSet(curMetaData.columns());
      Set<ColumnMetadata> reqCols = Sets.newHashSet(((CreateTableStatement)this.createTableStmt.get()).toTableMetadata().columns());
      return Sets.difference(reqCols, curCols);
   }

   protected Set<ColumnMetadata> reportUnexpectedColumns(TableMetadata curMetaData) {
      Set<ColumnMetadata> curCols = Sets.newHashSet(curMetaData.columns());
      Set<ColumnMetadata> reqCols = Sets.newHashSet(((CreateTableStatement)this.createTableStmt.get()).toTableMetadata().columns());
      return Sets.difference(curCols, reqCols);
   }

   protected Map<String, InconsistentValue<Object>> reportDifferentParams(TableMetadata curMetaData) {
      TableParams curParams = curMetaData.params;
      TableParams expParams = ((CreateTableStatement)this.createTableStmt.get()).toTableMetadata().params;
      Map<String, InconsistentValue<Object>> inconsistentParams = new HashMap();
      this.maybeReport(inconsistentParams, "bloom_filter_fp_chance", Double.valueOf(curParams.bloomFilterFpChance), Double.valueOf(expParams.bloomFilterFpChance));
      this.maybeReport(inconsistentParams, "caching", curParams.caching, expParams.caching);
      this.maybeReport(inconsistentParams, "compaction", curParams.compaction, expParams.compaction);
      this.maybeReport(inconsistentParams, "compression", curParams.compression, expParams.compression);
      this.maybeReport(inconsistentParams, "crcCheckChance", Double.valueOf(curParams.crcCheckChance), Double.valueOf(expParams.crcCheckChance));
      this.maybeReport(inconsistentParams, "dclocal_read_repair_chance", Double.valueOf(curParams.dcLocalReadRepairChance), Double.valueOf(expParams.dcLocalReadRepairChance));
      this.maybeReport(inconsistentParams, "default_time_to_live", Integer.valueOf(curParams.defaultTimeToLive), Integer.valueOf(expParams.defaultTimeToLive));
      this.maybeReport(inconsistentParams, "gc_grace_seconds", Integer.valueOf(curParams.gcGraceSeconds), Integer.valueOf(expParams.gcGraceSeconds));
      this.maybeReport(inconsistentParams, "max_index_interval", Integer.valueOf(curParams.maxIndexInterval), Integer.valueOf(expParams.maxIndexInterval));
      this.maybeReport(inconsistentParams, "memtable_flush_period_in_ms", Integer.valueOf(curParams.memtableFlushPeriodInMs), Integer.valueOf(expParams.memtableFlushPeriodInMs));
      this.maybeReport(inconsistentParams, "min_index_interval", Integer.valueOf(curParams.minIndexInterval), Integer.valueOf(expParams.minIndexInterval));
      this.maybeReport(inconsistentParams, "read_repair_chance", Double.valueOf(curParams.readRepairChance), Double.valueOf(expParams.readRepairChance));
      this.maybeReport(inconsistentParams, "speculative_retry", curParams.speculativeRetry, expParams.speculativeRetry);
      return inconsistentParams;
   }

   public static class TableValidationReport {
      public final String keyspace;
      public final String table;
      public final boolean valid;
      public final Map<String, InconsistentValue<Object>> inconsistentParams;
      public final Set<ColumnMetadata> missingColumns;
      public final Set<ColumnMetadata> unexpectedColumns;

      private TableValidationReport(String keyspace, String table, boolean valid, Map<String, InconsistentValue<Object>> inconsistentParams, Set<ColumnMetadata> missingColumns, Set<ColumnMetadata> unexpectedColumns) {
         this.keyspace = keyspace;
         this.table = table;
         this.valid = valid;
         this.inconsistentParams = Collections.unmodifiableMap(inconsistentParams);
         this.missingColumns = Collections.unmodifiableSet(missingColumns);
         this.unexpectedColumns = Collections.unmodifiableSet(unexpectedColumns);
      }

      public String toString() {
         StringBuilder report = (new StringBuilder("Validation report of table ")).append(this.keyspace).append(".").append(this.table).append(": ");
         if(this.valid) {
            report.append("OK");
         } else {
            report.append("Invalid");
         }

         if(!this.inconsistentParams.isEmpty()) {
            report.append("\nInconsistent params:\n");
            report.append((String)this.inconsistentParams.entrySet().stream().map((i) -> {
               return String.format(" - %s: %s", new Object[]{i.getKey(), i.getValue()});
            }).reduce((a, b) -> {
               return a + "\n" + b;
            }).orElse(""));
         }

         if(!this.missingColumns.isEmpty()) {
            report.append("\nMissing columns:\n");
            report.append((String)this.missingColumns.stream().map((c) -> {
               return " - " + c;
            }).reduce((a, b) -> {
               return a + "\n" + b;
            }).orElse(""));
         }

         if(!this.unexpectedColumns.isEmpty()) {
            report.append("\nUnexpected columns:\n");
            report.append((String)this.unexpectedColumns.stream().map((c) -> {
               return " - " + c;
            }).reduce((a, b) -> {
               return a + "\n" + b;
            }).orElse(""));
         }

         return report.toString();
      }
   }
}
