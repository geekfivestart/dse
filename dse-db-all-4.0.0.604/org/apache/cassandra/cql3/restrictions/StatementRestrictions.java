package org.apache.cassandra.cql3.restrictions;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.ImmutableSet.Builder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SystemKeyspacesFilteringRestrictions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.btree.BTreeSet;

public class StatementRestrictions {
   public static final String REQUIRES_ALLOW_FILTERING_MESSAGE = "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";
   private final StatementType type;
   public final TableMetadata table;
   protected final PartitionKeyRestrictions partitionKeyRestrictions;
   private final ClusteringColumnRestrictions clusteringColumnsRestrictions;
   private final RestrictionSet nonPrimaryKeyRestrictions;
   private final ImmutableSet<ColumnMetadata> notNullColumns;
   private final IndexRestrictions filterRestrictions;
   private final ImmutableList<AuthRestriction> authRestrictions;
   protected boolean usesSecondaryIndexing;
   protected boolean isKeyRange;
   private boolean hasRegularColumnsRestrictions;

   public static StatementRestrictions empty(StatementType type, TableMetadata table) {
      return new StatementRestrictions(type, table, false);
   }

   private StatementRestrictions(StatementType type, TableMetadata table, boolean allowFiltering) {
      this.type = type;
      this.table = table;
      this.partitionKeyRestrictions = new PartitionKeySingleRestrictionSet(table.partitionKeyAsClusteringComparator());
      this.clusteringColumnsRestrictions = new ClusteringColumnRestrictions(table, allowFiltering);
      this.nonPrimaryKeyRestrictions = new RestrictionSet();
      this.notNullColumns = ImmutableSet.of();
      this.filterRestrictions = IndexRestrictions.of();
      this.authRestrictions = ImmutableList.of();
   }

   private StatementRestrictions(StatementType type, TableMetadata table, PartitionKeyRestrictions partitionKeyRestrictions, ClusteringColumnRestrictions clusteringColumnsRestrictions, RestrictionSet nonPrimaryKeyRestrictions, ImmutableSet<ColumnMetadata> notNullColumns, boolean usesSecondaryIndexing, boolean isKeyRange, IndexRestrictions filterRestrictions, ImmutableList<AuthRestriction> authRestrictions) {
      this.type = type;
      this.table = table;
      this.partitionKeyRestrictions = partitionKeyRestrictions;
      this.clusteringColumnsRestrictions = clusteringColumnsRestrictions;
      this.nonPrimaryKeyRestrictions = nonPrimaryKeyRestrictions;
      this.notNullColumns = notNullColumns;
      this.usesSecondaryIndexing = usesSecondaryIndexing;
      this.isKeyRange = isKeyRange;
      this.filterRestrictions = filterRestrictions;
      this.authRestrictions = authRestrictions;
   }

   public StatementRestrictions addIndexRestrictions(Restrictions restrictions) {
      IndexRestrictions newIndexRestrictions = IndexRestrictions.builder().add(this.filterRestrictions).add(restrictions).build();
      return new StatementRestrictions(this.type, this.table, this.partitionKeyRestrictions, this.clusteringColumnsRestrictions, this.nonPrimaryKeyRestrictions, this.notNullColumns, this.usesSecondaryIndexing, this.isKeyRange, newIndexRestrictions, this.authRestrictions);
   }

   public StatementRestrictions addExternalRestrictions(Iterable<ExternalRestriction> restrictions) {
      IndexRestrictions.Builder newIndexRestrictions = IndexRestrictions.builder().add(this.filterRestrictions);
      Iterator var3 = restrictions.iterator();

      while(var3.hasNext()) {
         ExternalRestriction restriction = (ExternalRestriction)var3.next();
         newIndexRestrictions.add(restriction);
      }

      return new StatementRestrictions(this.type, this.table, this.partitionKeyRestrictions, this.clusteringColumnsRestrictions, this.nonPrimaryKeyRestrictions, this.notNullColumns, this.usesSecondaryIndexing, this.isKeyRange, newIndexRestrictions.build(), this.authRestrictions);
   }

   public StatementRestrictions(StatementType type, TableMetadata table, WhereClause whereClause, VariableSpecifications boundNames, boolean selectsOnlyStaticColumns, boolean allowFiltering, boolean forView) {
      this.type = type;
      this.table = table;
      SecondaryIndexManager secondaryIndexManager = null;
      if(type.allowUseOfSecondaryIndices()) {
         ColumnFamilyStore cfs = Keyspace.open(table.keyspace).getColumnFamilyStore(table.name);
         secondaryIndexManager = cfs.indexManager;
      }

      PartitionKeyRestrictions partitionKeyRestrictionSet = new PartitionKeySingleRestrictionSet(table.partitionKeyAsClusteringComparator());
      ClusteringColumnRestrictions clusteringColumnsRestrictionSet = new ClusteringColumnRestrictions(table, allowFiltering);
      RestrictionSet nonPrimaryKeyRestrictionSet = new RestrictionSet();
      Builder<ColumnMetadata> notNullColumnsBuilder = ImmutableSet.builder();
      Iterator var14 = whereClause.relations.iterator();

      while(true) {
         while(var14.hasNext()) {
            Relation relation = (Relation)var14.next();
            ColumnMetadata def;
            if(relation.operator() != Operator.IS_NOT) {
               Restriction restriction = relation.toRestriction(table, boundNames);
               if(relation.isLIKE() && (!type.allowUseOfSecondaryIndices() || !restriction.hasSupportingIndex(secondaryIndexManager))) {
                  throw RequestValidations.invalidRequest("LIKE restriction is only supported on properly indexed columns. %s is not valid.", new Object[]{relation.toString()});
               }

               def = restriction.getFirstColumn();
               if(def.isPartitionKey()) {
                  partitionKeyRestrictionSet = ((PartitionKeyRestrictions)partitionKeyRestrictionSet).mergeWith(restriction);
               } else if(def.isClusteringColumn()) {
                  clusteringColumnsRestrictionSet = clusteringColumnsRestrictionSet.mergeWith(restriction);
               } else {
                  nonPrimaryKeyRestrictionSet = nonPrimaryKeyRestrictionSet.addRestriction((SingleRestriction)restriction);
               }
            } else {
               if(!forView) {
                  throw RequestValidations.invalidRequest("Unsupported restriction: %s", new Object[]{relation});
               }

               Iterator var16 = relation.toRestriction(table, boundNames).getColumnDefs().iterator();

               while(var16.hasNext()) {
                  def = (ColumnMetadata)var16.next();
                  notNullColumnsBuilder.add(def);
               }
            }
         }

         this.partitionKeyRestrictions = (PartitionKeyRestrictions)partitionKeyRestrictionSet;
         this.clusteringColumnsRestrictions = clusteringColumnsRestrictionSet;
         this.nonPrimaryKeyRestrictions = nonPrimaryKeyRestrictionSet;
         this.notNullColumns = notNullColumnsBuilder.build();
         this.hasRegularColumnsRestrictions = this.nonPrimaryKeyRestrictions.hasRestrictionFor(ColumnMetadata.Kind.REGULAR);
         boolean hasQueriableClusteringColumnIndex = false;
         boolean hasQueriableIndex = false;
         IndexRestrictions.Builder filterRestrictionsBuilder = IndexRestrictions.builder();
         if(type.allowUseOfSecondaryIndices()) {
            if(whereClause.containsCustomExpressions()) {
               CustomIndexExpression customExpression = this.prepareCustomIndexExpression(whereClause.expressions, boundNames, secondaryIndexManager);
               filterRestrictionsBuilder.add((ExternalRestriction)customExpression);
            }

            hasQueriableClusteringColumnIndex = this.clusteringColumnsRestrictions.hasSupportingIndex(secondaryIndexManager);
            hasQueriableIndex = whereClause.containsCustomExpressions() || hasQueriableClusteringColumnIndex || this.partitionKeyRestrictions.hasSupportingIndex(secondaryIndexManager) || this.nonPrimaryKeyRestrictions.hasSupportingIndex(secondaryIndexManager);
         }

         this.processPartitionKeyRestrictions(hasQueriableIndex, allowFiltering, forView);
         if(this.usesSecondaryIndexing || this.partitionKeyRestrictions.needFiltering(table)) {
            filterRestrictionsBuilder.add((Restrictions)this.partitionKeyRestrictions);
         }

         if(selectsOnlyStaticColumns && this.hasClusteringColumnsRestrictions() && (type.isDelete() || type.isUpdate())) {
            throw RequestValidations.invalidRequest("Invalid restrictions on clustering columns since the %s statement modifies only static columns", new Object[]{type});
         }

         this.processClusteringColumnsRestrictions(hasQueriableIndex, selectsOnlyStaticColumns, forView, allowFiltering);
         if(this.isKeyRange && hasQueriableClusteringColumnIndex) {
            this.usesSecondaryIndexing = true;
         }

         if(this.usesSecondaryIndexing || this.clusteringColumnsRestrictions.needFiltering()) {
            filterRestrictionsBuilder.add((Restrictions)this.clusteringColumnsRestrictions);
         }

         if(!this.nonPrimaryKeyRestrictions.isEmpty()) {
            if(!type.allowNonPrimaryKeyInWhereClause()) {
               Collection<ColumnIdentifier> nonPrimaryKeyColumns = ColumnMetadata.toIdentifiers(this.nonPrimaryKeyRestrictions.getColumnDefs());
               throw RequestValidations.invalidRequest("Non PRIMARY KEY columns found in where clause: %s ", new Object[]{Joiner.on(", ").join(nonPrimaryKeyColumns)});
            }

            if(hasQueriableIndex) {
               this.usesSecondaryIndexing = true;
            } else if(!allowFiltering) {
               throw RequestValidations.invalidRequest("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING");
            }

            filterRestrictionsBuilder.add((Restrictions)this.nonPrimaryKeyRestrictions);
         }

         this.filterRestrictions = filterRestrictionsBuilder.build();
         if(this.usesSecondaryIndexing) {
            this.validateSecondaryIndexSelections();
         }

         this.authRestrictions = getAuthRestrictions(table, type);
         return;
      }
   }

   private static ImmutableList<AuthRestriction> getAuthRestrictions(TableMetadata table, StatementType type) {
      if(!type.isSelect()) {
         return ImmutableList.of();
      } else {
         AuthRestriction restriction = SystemKeyspacesFilteringRestrictions.restrictionsForTable(table);
         return restriction == null?ImmutableList.of():ImmutableList.of(restriction);
      }
   }

   public void addFunctionsTo(List<Function> functions) {
      this.partitionKeyRestrictions.addFunctionsTo(functions);
      this.clusteringColumnsRestrictions.addFunctionsTo(functions);
      this.nonPrimaryKeyRestrictions.addFunctionsTo(functions);
   }

   public IndexRestrictions getIndexRestrictions() {
      return this.filterRestrictions;
   }

   public Set<ColumnMetadata> nonPKRestrictedColumns(boolean includeNotNullRestrictions) {
      Set<ColumnMetadata> columns = new HashSet();
      UnmodifiableIterator var3 = this.filterRestrictions.getRestrictions().iterator();

      while(var3.hasNext()) {
         Restrictions r = (Restrictions)var3.next();
         Iterator var5 = r.getColumnDefs().iterator();

         while(var5.hasNext()) {
            ColumnMetadata def = (ColumnMetadata)var5.next();
            if(!def.isPrimaryKeyColumn()) {
               columns.add(def);
            }
         }
      }

      if(includeNotNullRestrictions) {
         var3 = this.notNullColumns.iterator();

         while(var3.hasNext()) {
            ColumnMetadata def = (ColumnMetadata)var3.next();
            if(!def.isPrimaryKeyColumn()) {
               columns.add(def);
            }
         }
      }

      return columns;
   }

   public ImmutableSet<ColumnMetadata> notNullColumns() {
      return this.notNullColumns;
   }

   public boolean isRestricted(ColumnMetadata column) {
      return this.notNullColumns.contains(column)?true:this.getRestrictions(column.kind).getColumnDefs().contains(column);
   }

   public boolean keyIsInRelation() {
      return this.partitionKeyRestrictions.hasIN();
   }

   public boolean isKeyRange() {
      return this.isKeyRange;
   }

   public boolean isColumnRestrictedByEq(ColumnMetadata columnDef) {
      Set<Restriction> restrictions = this.getRestrictions(columnDef.kind).getRestrictions(columnDef);
      Stream var10000 = restrictions.stream();
      SingleRestriction.class.getClass();
      return var10000.filter(SingleRestriction.class::isInstance).anyMatch((p) -> {
         return ((SingleRestriction)p).isEQ();
      });
   }

   protected Restrictions getRestrictions(ColumnMetadata.Kind kind) {
      switch (kind) {
         case PARTITION_KEY: {
            return this.partitionKeyRestrictions;
         }
         case CLUSTERING: {
            return this.clusteringColumnsRestrictions;
         }
      }
      return this.nonPrimaryKeyRestrictions;
   }

   public boolean usesSecondaryIndexing() {
      return this.usesSecondaryIndexing;
   }

   protected void processPartitionKeyRestrictions(boolean hasQueriableIndex, boolean allowFiltering, boolean forView) {
      if(!this.type.allowPartitionKeyRanges()) {
         RequestValidations.checkFalse(this.partitionKeyRestrictions.isOnToken(), "The token function cannot be used in WHERE clauses for %s statements", this.type);
         if(this.partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(this.table)) {
            throw RequestValidations.invalidRequest("Some partition key parts are missing: %s", new Object[]{Joiner.on(", ").join(this.getPartitionKeyUnrestrictedComponents())});
         }

         RequestValidations.checkFalse(this.partitionKeyRestrictions.hasSlice(), "Only EQ and IN relation are supported on the partition key (unless you use the token() function) for %s statements", this.type);
      } else {
         if(this.partitionKeyRestrictions.isOnToken()) {
            this.isKeyRange = true;
         }

         if(this.partitionKeyRestrictions.isEmpty() && this.partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(this.table)) {
            this.isKeyRange = true;
            this.usesSecondaryIndexing = hasQueriableIndex;
         }

         if(this.partitionKeyRestrictions.needFiltering(this.table)) {
            if(!allowFiltering && !forView && !hasQueriableIndex) {
               throw new InvalidRequestException("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING");
            }

            if(this.partitionKeyRestrictions.hasIN()) {
               throw new InvalidRequestException("IN restrictions are not supported when the query involves filtering");
            }

            this.isKeyRange = true;
            this.usesSecondaryIndexing = hasQueriableIndex;
         }
      }

   }

   public boolean hasPartitionKeyRestrictions() {
      return !this.partitionKeyRestrictions.isEmpty();
   }

   public boolean hasNonPrimaryKeyRestrictions() {
      return !this.nonPrimaryKeyRestrictions.isEmpty();
   }

   private Collection<ColumnIdentifier> getPartitionKeyUnrestrictedComponents() {
      List<ColumnMetadata> list = new ArrayList(this.table.partitionKeyColumns());
      list.removeAll(this.partitionKeyRestrictions.getColumnDefs());
      return ColumnMetadata.toIdentifiers(list);
   }

   public boolean isPartitionKeyRestrictionsOnToken() {
      return this.partitionKeyRestrictions.isOnToken();
   }

   public boolean clusteringKeyRestrictionsHasIN() {
      return this.clusteringColumnsRestrictions.hasIN();
   }

   private void processClusteringColumnsRestrictions(boolean hasQueriableIndex, boolean selectsOnlyStaticColumns, boolean forView, boolean allowFiltering) {
      RequestValidations.checkFalse(!this.type.allowClusteringColumnSlices() && this.clusteringColumnsRestrictions.hasSlice(), "Slice restrictions are not supported on the clustering columns in %s statements", this.type);
      if(this.type.allowClusteringColumnSlices() || this.table.isCompactTable() && (!this.table.isCompactTable() || this.hasClusteringColumnsRestrictions())) {
         RequestValidations.checkFalse(this.clusteringColumnsRestrictions.hasContains() && !hasQueriableIndex && !allowFiltering, "Clustering columns can only be restricted with CONTAINS with a secondary index or filtering");
         if(this.hasClusteringColumnsRestrictions() && this.clusteringColumnsRestrictions.needFiltering()) {
            if(!hasQueriableIndex && !forView) {
               if(!allowFiltering) {
                  List<ColumnMetadata> clusteringColumns = this.table.clusteringColumns();
                  List<ColumnMetadata> restrictedColumns = new LinkedList(this.clusteringColumnsRestrictions.getColumnDefs());
                  int i = 0;

                  for(int m = restrictedColumns.size(); i < m; ++i) {
                     ColumnMetadata clusteringColumn = (ColumnMetadata)clusteringColumns.get(i);
                     ColumnMetadata restrictedColumn = (ColumnMetadata)restrictedColumns.get(i);
                     if(!clusteringColumn.equals(restrictedColumn)) {
                        throw RequestValidations.invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted", new Object[]{restrictedColumn.name, clusteringColumn.name});
                     }
                  }
               }
            } else {
               this.usesSecondaryIndexing = true;
            }
         }
      } else if(!selectsOnlyStaticColumns && this.hasUnrestrictedClusteringColumns()) {
         throw RequestValidations.invalidRequest("Some clustering keys are missing: %s", new Object[]{Joiner.on(", ").join(this.getUnrestrictedClusteringColumns())});
      }

   }

   private Collection<ColumnIdentifier> getUnrestrictedClusteringColumns() {
      List<ColumnMetadata> missingClusteringColumns = new ArrayList(this.table.clusteringColumns());
      missingClusteringColumns.removeAll(new LinkedList(this.clusteringColumnsRestrictions.getColumnDefs()));
      return ColumnMetadata.toIdentifiers(missingClusteringColumns);
   }

   private boolean hasUnrestrictedClusteringColumns() {
      return this.table.clusteringColumns().size() != this.clusteringColumnsRestrictions.size();
   }

   private CustomIndexExpression prepareCustomIndexExpression(List<CustomIndexExpression> expressions, VariableSpecifications boundNames, SecondaryIndexManager indexManager) {
      if(expressions.size() > 1) {
         throw new InvalidRequestException("Multiple custom index expressions in a single query are not supported");
      } else {
         CustomIndexExpression expression = (CustomIndexExpression)expressions.get(0);
         CFName cfName = expression.targetIndex.getCfName();
         if(cfName.hasKeyspace() && !expression.targetIndex.getKeyspace().equals(this.table.keyspace)) {
            throw IndexRestrictions.invalidIndex(expression.targetIndex, this.table);
         } else if(cfName.getColumnFamily() != null && !cfName.getColumnFamily().equals(this.table.name)) {
            throw IndexRestrictions.invalidIndex(expression.targetIndex, this.table);
         } else if(!this.table.indexes.has(expression.targetIndex.getIdx())) {
            throw IndexRestrictions.indexNotFound(expression.targetIndex, this.table);
         } else {
            Index index = indexManager.getIndex((IndexMetadata)this.table.indexes.get(expression.targetIndex.getIdx()).get());
            if(!index.getIndexMetadata().isCustom()) {
               throw IndexRestrictions.nonCustomIndexInExpression(expression.targetIndex);
            } else {
               AbstractType<?> expressionType = index.customExpressionValueType();
               if(expressionType == null) {
                  throw IndexRestrictions.customExpressionNotSupported(expression.targetIndex);
               } else {
                  expression.prepareValue(this.table, expressionType, boundNames);
                  return expression;
               }
            }
         }
      }
   }

   public RowFilter getRowFilter(SecondaryIndexManager indexManager, QueryState state, QueryOptions options) {
      if(this.filterRestrictions.isEmpty() && this.authRestrictions.isEmpty()) {
         return RowFilter.NONE;
      } else {
         RowFilter filter = RowFilter.create();
         UnmodifiableIterator var5 = this.filterRestrictions.getRestrictions().iterator();

         while(var5.hasNext()) {
            Restrictions restrictions = (Restrictions)var5.next();
            restrictions.addRowFilterTo(filter, indexManager, options);
         }

         var5 = this.filterRestrictions.getExternalExpressions().iterator();

         while(var5.hasNext()) {
            ExternalRestriction expression = (ExternalRestriction)var5.next();
            expression.addToRowFilter(filter, this.table, options);
         }

         var5 = this.authRestrictions.iterator();

         while(var5.hasNext()) {
            AuthRestriction restriction = (AuthRestriction)var5.next();
            restriction.addRowFilterTo(filter, state);
         }

         return filter;
      }
   }

   public List<ByteBuffer> getPartitionKeys(QueryOptions options) {
      return this.partitionKeyRestrictions.values(options);
   }

   private ByteBuffer getPartitionKeyBound(Bound b, QueryOptions options) {
      return (ByteBuffer)this.partitionKeyRestrictions.bounds(b, options).get(0);
   }

   public AbstractBounds<PartitionPosition> getPartitionKeyBounds(QueryOptions options) {
      IPartitioner p = this.table.partitioner;
      return this.partitionKeyRestrictions.isOnToken()?this.getPartitionKeyBoundsForTokenRestrictions(p, options):this.getPartitionKeyBounds(p, options);
   }

   private AbstractBounds<PartitionPosition> getPartitionKeyBounds(IPartitioner p, QueryOptions options) {
      if(this.partitionKeyRestrictions.needFiltering(this.table)) {
         return new Range(p.getMinimumToken().minKeyBound(), p.getMinimumToken().maxKeyBound());
      } else {
         ByteBuffer startKeyBytes = this.getPartitionKeyBound(Bound.START, options);
         ByteBuffer finishKeyBytes = this.getPartitionKeyBound(Bound.END, options);
         PartitionPosition startKey = PartitionPosition.ForKey.get(startKeyBytes, p);
         PartitionPosition finishKey = PartitionPosition.ForKey.get(finishKeyBytes, p);
         return (AbstractBounds)(startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum()?null:(this.partitionKeyRestrictions.isInclusive(Bound.START)?(this.partitionKeyRestrictions.isInclusive(Bound.END)?new Bounds(startKey, finishKey):new IncludingExcludingBounds(startKey, finishKey)):(this.partitionKeyRestrictions.isInclusive(Bound.END)?new Range(startKey, finishKey):new ExcludingBounds(startKey, finishKey))));
      }
   }

   private AbstractBounds<PartitionPosition> getPartitionKeyBoundsForTokenRestrictions(IPartitioner p, QueryOptions options) {
      Token startToken = this.getTokenBound(Bound.START, options, p);
      Token endToken = this.getTokenBound(Bound.END, options, p);
      boolean includeStart = this.partitionKeyRestrictions.isInclusive(Bound.START);
      boolean includeEnd = this.partitionKeyRestrictions.isInclusive(Bound.END);
      int cmp = startToken.compareTo(endToken);
      if(!startToken.isMinimum() && !endToken.isMinimum() && (cmp > 0 || cmp == 0 && (!includeStart || !includeEnd))) {
         return null;
      } else {
         PartitionPosition start = includeStart?startToken.minKeyBound():startToken.maxKeyBound();
         PartitionPosition end = includeEnd?endToken.maxKeyBound():endToken.minKeyBound();
         return new Range(start, end);
      }
   }

   private Token getTokenBound(Bound b, QueryOptions options, IPartitioner p) {
      if(!this.partitionKeyRestrictions.hasBound(b)) {
         return p.getMinimumToken();
      } else {
         ByteBuffer value = (ByteBuffer)this.partitionKeyRestrictions.bounds(b, options).get(0);
         RequestValidations.checkNotNull(value, "Invalid null token value");
         return p.getTokenFactory().fromByteArray(value);
      }
   }

   public boolean hasClusteringColumnsRestrictions() {
      return !this.clusteringColumnsRestrictions.isEmpty();
   }

   public NavigableSet<Clustering> getClusteringColumns(QueryOptions options) {
      return (NavigableSet)(this.table.isStaticCompactTable()?BTreeSet.empty(this.table.comparator):this.clusteringColumnsRestrictions.valuesAsClustering(options));
   }

   public NavigableSet<ClusteringBound> getClusteringColumnsBounds(Bound b, QueryOptions options) {
      return this.clusteringColumnsRestrictions.boundsAsClustering(b, options);
   }

   public boolean isColumnRange() {
      int numberOfClusteringColumns = this.table.isStaticCompactTable()?0:this.table.clusteringColumns().size();
      return this.clusteringColumnsRestrictions.size() < numberOfClusteringColumns || !this.clusteringColumnsRestrictions.hasOnlyEqualityRestrictions();
   }

   public boolean needFiltering() {
      int numberOfRestrictions = this.filterRestrictions.getExternalExpressions().size();

      Restrictions restrictions;
      for(UnmodifiableIterator var2 = this.filterRestrictions.getRestrictions().iterator(); var2.hasNext(); numberOfRestrictions += restrictions.size()) {
         restrictions = (Restrictions)var2.next();
      }

      return numberOfRestrictions > 1 || numberOfRestrictions == 0 && !this.clusteringColumnsRestrictions.isEmpty() || numberOfRestrictions != 0 && this.nonPrimaryKeyRestrictions.hasMultipleContains();
   }

   protected void validateSecondaryIndexSelections() {
      RequestValidations.checkFalse(this.keyIsInRelation(), "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
   }

   public boolean hasAllPKColumnsRestrictedByEqualities() {
      return !this.isPartitionKeyRestrictionsOnToken() && !this.partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(this.table) && this.partitionKeyRestrictions.hasOnlyEqualityRestrictions() && !this.hasUnrestrictedClusteringColumns() && this.clusteringColumnsRestrictions.hasOnlyEqualityRestrictions();
   }

   public boolean hasRegularColumnsRestrictions() {
      return this.hasRegularColumnsRestrictions;
   }
}
