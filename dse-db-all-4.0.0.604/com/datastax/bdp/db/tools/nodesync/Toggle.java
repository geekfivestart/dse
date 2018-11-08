package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;

public abstract class Toggle extends NodeSyncCommand {
   private static final String ALTER_QUERY_FORMAT = "ALTER TABLE %s WITH nodesync = {'enabled':'%s'}";
   @Option(
      name = {"-k", "--keyspace"},
      description = "Default keyspace to be used with unqualified table names and wildcards"
   )
   String defaultKeyspace = null;
   @Arguments(
      usage = "<table> [<table>...]",
      required = true,
      description = "One or many qualified table names such as \"my_keyspace.my_table\", or unqualifed table names such as \"my_table\" relying of the default keyspace, or wildcards (*) meaning all the user-alterable tables either in the default keyspace or in the whole cluster"
   )
   List<String> tableSelectors = new ArrayList();

   public Toggle() {
   }

   protected final void execute(Metadata metadata, Session session, NodeProbes probes) {
      List futures = (List)this.tableSelectors.stream().map((selector) -> {
         return this.tablesMetadata(metadata, selector);
      }).flatMap(Collection::stream).distinct().map(NodeSyncCommand::fullyQualifiedTableName).map((t) -> {
         return this.execute(session, t);
      }).collect(Collectors.toList());

      try {
         FBUtilities.waitOnFutures(futures);
      } catch (Exception var6) {
         throw new NodeSyncException("Some operations have failed");
      }
   }

   private ListenableFuture<ResultSet> execute(Session session, final String table) {
      ListenableFuture<ResultSet> future = session.executeAsync(this.alterQuery(table));
      final SettableFuture<ResultSet> result = SettableFuture.create();
      Futures.addCallback(future, new FutureCallback<ResultSet>() {
         public void onSuccess(@Nullable ResultSet rs) {
            Toggle.this.printVerbose(Toggle.this.successMessage(table), new Object[0]);
            result.set(rs);
         }

         public void onFailure(Throwable cause) {
            System.err.println(Toggle.this.failureMessage(table, cause));
            result.setException(cause);
         }
      });
      return result;
   }

   protected abstract String alterQuery(String var1);

   protected abstract String successMessage(String var1);

   protected abstract String failureMessage(String var1, Throwable var2);

   private Collection<? extends AbstractTableMetadata> tablesMetadata(Metadata metadata, String tableSelector) {
      if(isWildcard(tableSelector)) {
         return this.defaultKeyspace != null?checkUserAlterable(parseKeyspace(metadata, this.defaultKeyspace)).getTables():(Collection)metadata.getKeyspaces().stream().filter(Toggle::isUserAlterable).map(KeyspaceMetadata::getTables).flatMap(Collection::stream).collect(Collectors.toSet());
      } else {
         AbstractTableMetadata tableMetadata = parseTable(metadata, this.defaultKeyspace, tableSelector);
         checkUserAlterable(tableMetadata.getKeyspace());
         return Collections.singleton(tableMetadata);
      }
   }

   private static boolean isWildcard(String tableSelector) {
      return tableSelector.equals("*");
   }

   private static KeyspaceMetadata checkUserAlterable(KeyspaceMetadata keyspace) {
      if(isUserAlterable(keyspace)) {
         return keyspace;
      } else {
         throw new NodeSyncException("Keyspace [" + keyspace.getName() + "] is not alterable.");
      }
   }

   private static boolean isUserAlterable(KeyspaceMetadata keyspace) {
      String name = keyspace.getName();
      return name.equals("system_distributed") || SchemaConstants.isUserKeyspace(name);
   }

   @Command(
      name = "disable",
      description = "Disable nodesync on the specified tables"
   )
   public static class Disable extends Toggle {
      public Disable() {
      }

      public String alterQuery(String table) {
         return String.format("ALTER TABLE %s WITH nodesync = {'enabled':'%s'}", new Object[]{table, Boolean.valueOf(false)});
      }

      protected String successMessage(String table) {
         return "Nodesync disabled for " + table;
      }

      protected String failureMessage(String table, Throwable cause) {
         return String.format("Error disabling nodesync for %s: %s", new Object[]{table, cause.getMessage()});
      }
   }

   @Command(
      name = "enable",
      description = "Enable nodesync on the specified tables"
   )
   public static class Enable extends Toggle {
      public Enable() {
      }

      public String alterQuery(String table) {
         return String.format("ALTER TABLE %s WITH nodesync = {'enabled':'%s'}", new Object[]{table, Boolean.valueOf(true)});
      }

      protected String successMessage(String table) {
         return "Nodesync enabled for " + table;
      }

      protected String failureMessage(String table, Throwable cause) {
         return String.format("Error enabling nodesync for %s: %s", new Object[]{table, cause.getMessage()});
      }
   }
}
