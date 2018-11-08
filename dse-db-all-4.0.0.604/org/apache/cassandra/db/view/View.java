package org.apache.cassandra.db.view;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.MultiColumnRelation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class View {
   private static final Logger logger = LoggerFactory.getLogger(View.class);
   public final String name;
   private volatile ViewMetadata definition;
   private final ColumnFamilyStore baseCfs;
   public volatile List<ColumnMetadata> baseNonPKColumnsInViewPK;
   private ViewBuilder builder;
   private final SelectStatement.RawStatement rawSelect;
   private SelectStatement select;
   private ReadQuery query;

   public View(ViewMetadata definition, ColumnFamilyStore baseCfs) {
      this.baseCfs = baseCfs;
      this.name = definition.name;
      this.rawSelect = definition.select;
      this.updateDefinition(definition);
   }

   public ViewMetadata getDefinition() {
      return this.definition;
   }

   public void updateDefinition(ViewMetadata definition) {
      this.definition = definition;
      List<ColumnMetadata> nonPKDefPartOfViewPK = new ArrayList();
      UnmodifiableIterator var3 = this.baseCfs.metadata.get().columns().iterator();

      while(var3.hasNext()) {
         ColumnMetadata baseColumn = (ColumnMetadata)var3.next();
         ColumnMetadata viewColumn = this.getViewColumn(baseColumn);
         if(viewColumn != null && !baseColumn.isPrimaryKeyColumn() && viewColumn.isPrimaryKeyColumn()) {
            nonPKDefPartOfViewPK.add(baseColumn);
         }
      }

      this.baseNonPKColumnsInViewPK = nonPKDefPartOfViewPK;
   }

   public ColumnMetadata getViewColumn(ColumnMetadata baseColumn) {
      return this.definition.metadata.getColumn(baseColumn.name);
   }

   public ColumnMetadata getBaseColumn(ColumnMetadata viewColumn) {
      ColumnMetadata baseColumn = this.baseCfs.metadata().getColumn(viewColumn.name);

      assert baseColumn != null;

      return baseColumn;
   }

   public boolean mayBeAffectedBy(DecoratedKey partitionKey, Row update) {
      return this.getReadQuery().selectsClustering(partitionKey, update.clustering());
   }

   public Flow<Boolean> matchesViewFilter(DecoratedKey partitionKey, Row baseRow, int nowInSec) {
      return !this.getReadQuery().selectsClustering(partitionKey, baseRow.clustering())?Flow.just(Boolean.valueOf(false)):this.getSelectStatement().rowFilterForInternalCalls().isSatisfiedBy(this.baseCfs.metadata(), partitionKey, baseRow, nowInSec);
   }

   public SelectStatement getSelectStatement() {
      if(this.select == null) {
         ClientState state = ClientState.forInternalCalls();
         state.setKeyspace(this.baseCfs.keyspace.getName());
         this.rawSelect.prepareKeyspace(state);
         ParsedStatement.Prepared prepared = this.rawSelect.prepare(true);
         this.select = (SelectStatement)prepared.statement;
      }

      return this.select;
   }

   public ReadQuery getReadQuery() {
      if(this.query == null) {
         this.query = this.getSelectStatement().getQuery(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(Collections.emptyList()), FBUtilities.nowInSeconds());
         logger.trace("View query: {}", this.rawSelect);
      }

      return this.query;
   }

   public synchronized void build() {
      this.stopBuild();
      this.builder = new ViewBuilder(this.baseCfs, this);
      this.builder.start();
   }

   synchronized void stopBuild() {
      if(this.builder != null) {
         logger.debug("Stopping current view builder due to schema change");
         this.builder.stop();
         this.builder = null;
      }

   }

   @Nullable
   public static TableMetadataRef findBaseTable(String keyspace, String viewName) {
      ViewMetadata view = Schema.instance.getView(keyspace, viewName);
      return view == null?null:Schema.instance.getTableMetadataRef(view.baseTableId);
   }

   public static Iterable<ViewMetadata> findAll(String keyspace, String baseTable) {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
      return Iterables.filter(ksm.views, (view) -> {
         return view.baseTableName.equals(baseTable);
      });
   }

   public static String buildSelectStatement(String cfName, Collection<ColumnMetadata> includedColumns, String whereClause) {
      StringBuilder rawSelect = new StringBuilder("SELECT ");
      if(includedColumns != null && !includedColumns.isEmpty()) {
         rawSelect.append((String)includedColumns.stream().map((id) -> {
            return id.name.toCQLString();
         }).collect(Collectors.joining(", ")));
      } else {
         rawSelect.append("*");
      }

      rawSelect.append(" FROM \"").append(cfName).append("\" WHERE ").append(whereClause).append(" ALLOW FILTERING");
      return rawSelect.toString();
   }

   public static String relationsToWhereClause(List<Relation> whereClause) {
      List<String> expressions = new ArrayList(whereClause.size());

      StringBuilder sb;
      for(Iterator var2 = whereClause.iterator(); var2.hasNext(); expressions.add(sb.toString())) {
         Relation rel = (Relation)var2.next();
         sb = new StringBuilder();
         if(rel.isMultiColumn()) {
            sb.append((String)((MultiColumnRelation)rel).getEntities().stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")")));
         } else {
            sb.append(((SingleColumnRelation)rel).getEntity());
         }

         sb.append(" ").append(rel.operator()).append(" ");
         if(rel.isIN()) {
            sb.append((String)rel.getInValues().stream().map(Term.Raw::getText).collect(Collectors.joining(", ", "(", ")")));
         } else {
            sb.append(rel.getValue().getText());
         }
      }

      return (String)expressions.stream().collect(Collectors.joining(" AND "));
   }

   public boolean hasSamePrimaryKeyColumnsAsBaseTable() {
      return this.baseNonPKColumnsInViewPK.isEmpty();
   }

   public boolean enforceStrictLiveness() {
      return !this.baseNonPKColumnsInViewPK.isEmpty();
   }
}
