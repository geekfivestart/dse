package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.commons.lang3.StringUtils;

public class CreateTableStatement extends SchemaAlteringStatement implements TableStatement {
   private static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
   private List<AbstractType<?>> keyTypes;
   private List<AbstractType<?>> clusteringTypes;
   private final Map<ByteBuffer, AbstractType> multicellColumns = new HashMap();
   private final List<ColumnIdentifier> keyAliases = new ArrayList();
   private final List<ColumnIdentifier> columnAliases = new ArrayList();
   private boolean isDense;
   private boolean isCompound;
   private boolean hasCounters;
   private final Map<ColumnIdentifier, AbstractType> columns = new TreeMap<ColumnIdentifier,AbstractType>((o1, o2) -> {
      return o1.bytes.compareTo(o2.bytes);
   });
   private final Set<ColumnIdentifier> staticColumns;
   private final TableParams params;
   private final boolean ifNotExists;
   private final TableId id;

   public CreateTableStatement(CFName name, TableParams params, boolean ifNotExists, Set<ColumnIdentifier> staticColumns, TableId id) {
      super(name);
      this.params = params;
      this.ifNotExists = ifNotExists;
      this.staticColumns = staticColumns;
      this.id = id;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.ADD_CF;
   }

   public void checkAccess(QueryState state) {
      state.checkKeyspacePermission(this.keyspace(), CorePermission.CREATE);
   }

   public void validate(QueryState state) {
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      if(this.id != null) {
         TableMetadata cfm = Schema.instance.getTableMetadata(this.id);
         if(cfm != null) {
            if(this.ifNotExists) {
               return Maybe.empty();
            }

            throw new AlreadyExistsException(this.keyspace(), this.columnFamily(), String.format("ID %s used in CREATE TABLE statement is already used by table %s.%s", new Object[]{this.id, cfm.keyspace, cfm.name}));
         }
      }

      if(this.params.compaction.klass().equals(DateTieredCompactionStrategy.class)) {
         DateTieredCompactionStrategy.deprecatedWarning(this.keyspace(), this.columnFamily());
      }

      if(TimeWindowCompactionStrategy.shouldLogNodeSyncSplitDuringFlushWarning(this.toTableMetadata(), this.params)) {
         ClientWarn.instance.warn(TimeWindowCompactionStrategy.getNodeSyncSplitDuringFlushWarning(this.keyspace(), this.columnFamily()));
      }

      return MigrationManager.announceNewTable(this.toTableMetadata(), isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily()))).onErrorResumeNext((e) -> {
         return e instanceof AlreadyExistsException && this.ifNotExists?Maybe.empty():Maybe.error(e);
      });
   }

   protected void grantPermissionsToCreator(QueryState state) {
      try {
         IResource resource = DataResource.table(this.keyspace(), this.columnFamily());
         IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
         RoleResource role = RoleResource.role(state.getClientState().getUser().getName());
         authorizer.grant(AuthenticatedUser.SYSTEM_USER, authorizer.applicablePermissions(resource), resource, role, new GrantMode[]{GrantMode.GRANT});
      } catch (RequestExecutionException var5) {
         throw new RuntimeException(var5);
      }
   }

   public static TableMetadata.Builder parse(String cql, String keyspace) {
      return parse(cql, keyspace, Collections.emptyList());
   }

   public static TableMetadata.Builder parse(String cql, String keyspace, Collection<UserType> types) {
      CreateTableStatement.RawStatement raw = (CreateTableStatement.RawStatement)CQLFragmentParser.parseAny(CqlParser::createTableStatement, cql, "CREATE TABLE");
      raw.prepareKeyspace(keyspace);
      CreateTableStatement prepared = (CreateTableStatement)raw.prepare(Types.of(types)).statement;
      return prepared.builder();
   }

   public TableMetadata.Builder builder() {
      TableMetadata.Builder builder = TableMetadata.builder(this.keyspace(), this.columnFamily());
      if(this.id != null) {
         builder.id(this.id);
      }

      builder.isDense(this.isDense).isCompound(this.isCompound).isCounter(this.hasCounters).isSuper(false).isView(false).params(this.params);

      int i;
      for(i = 0; i < this.keyAliases.size(); ++i) {
         builder.addPartitionKeyColumn((ColumnIdentifier)this.keyAliases.get(i), (AbstractType)this.keyTypes.get(i));
      }

      for(i = 0; i < this.columnAliases.size(); ++i) {
         builder.addClusteringColumn((ColumnIdentifier)this.columnAliases.get(i), (AbstractType)this.clusteringTypes.get(i));
      }

      boolean isStaticCompact = !this.isDense && !this.isCompound;
      Iterator var3 = this.columns.entrySet().iterator();

      while(true) {
         while(var3.hasNext()) {
            Entry<ColumnIdentifier, AbstractType> entry = (Entry)var3.next();
            ColumnIdentifier name = (ColumnIdentifier)entry.getKey();
            if(!this.staticColumns.contains(name) && !isStaticCompact) {
               builder.addRegularColumn(name, (AbstractType)entry.getValue());
            } else {
               builder.addStaticColumn(name, (AbstractType)entry.getValue());
            }
         }

         boolean isCompactTable = this.isDense || !this.isCompound;
         if(isCompactTable) {
            CompactTables.DefaultNames names = CompactTables.defaultNameGenerator(builder.columnNames());
            if(isStaticCompact) {
               builder.addClusteringColumn((String)names.defaultClusteringName(), UTF8Type.instance);
               builder.addRegularColumn((String)names.defaultCompactValueName(), (AbstractType)(this.hasCounters?CounterColumnType.instance:BytesType.instance));
            } else if(this.isDense && !builder.hasRegularColumns()) {
               builder.addRegularColumn((String)names.defaultCompactValueName(), EmptyType.instance);
            }
         }

         return builder;
      }
   }

   public TableMetadata toTableMetadata() {
      return this.builder().build();
   }

   public static class RawStatement extends CFStatement {
      private final Map<ColumnIdentifier, CQL3Type.Raw> definitions = new HashMap();
      public final CFProperties properties = new CFProperties();
      private final List<List<ColumnIdentifier>> keyAliases = new ArrayList();
      private final List<ColumnIdentifier> columnAliases = new ArrayList();
      private final Set<ColumnIdentifier> staticColumns = new HashSet();
      private final Multiset<ColumnIdentifier> definedNames = HashMultiset.create(1);
      private final boolean ifNotExists;

      public RawStatement(CFName name, boolean ifNotExists) {
         super(name);
         this.ifNotExists = ifNotExists;
      }

      public ParsedStatement.Prepared prepare() throws RequestValidationException {
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.keyspace());
         if(ksm == null) {
            throw new ConfigurationException(String.format("Keyspace %s doesn't exist", new Object[]{this.keyspace()}));
         } else {
            return this.prepare(ksm.types);
         }
      }

      public ParsedStatement.Prepared prepare(Types udts) throws RequestValidationException {
         boolean useCompactStorage;
         if (!PATTERN_WORD_CHARS.matcher(this.columnFamily()).matches()) {
            throw new InvalidRequestException(String.format("\"%s\" is not a valid table name (must be alphanumeric character or underscore only: [a-zA-Z_0-9]+)", this.columnFamily()));
         }
         if (this.columnFamily().length() > 48) {
            throw new InvalidRequestException(String.format("Table names shouldn't be more than %s characters long (got \"%s\")", 48, this.columnFamily()));
         }
         for (Multiset.Entry entry : this.definedNames.entrySet()) {
            if (entry.getCount() <= 1) continue;
            throw new InvalidRequestException(String.format("Multiple definition of identifier %s", entry.getElement()));
         }
         this.properties.validate();
         TableParams params = this.properties.properties.asNewTableParams();
         CreateTableStatement stmt = new CreateTableStatement(this.cfName, params, this.ifNotExists, this.staticColumns, this.properties.properties.getId());
         for (Map.Entry<ColumnIdentifier, CQL3Type.Raw> entry : this.definitions.entrySet()) {
            ColumnIdentifier id = entry.getKey();
            CQL3Type pt = entry.getValue().prepare(this.keyspace(), udts);
            if (pt.getType().isMultiCell()) {
               stmt.multicellColumns.put(id.bytes, pt.getType());
            }
            if (entry.getValue().isCounter()) {
               stmt.hasCounters = true;
            }
            if (pt.getType().isUDT() && pt.getType().isMultiCell()) {
               for (AbstractType<?> innerType : ((UserType)pt.getType()).fieldTypes()) {
                  if (!innerType.isMultiCell()) continue;
                  assert (innerType.isCollection());
                  throw new InvalidRequestException("Non-frozen UDTs with nested non-frozen collections are not supported");
               }
            }
            stmt.columns.put(id, pt.getType());
         }
         if (this.keyAliases.isEmpty()) {
            throw new InvalidRequestException("No PRIMARY KEY specifed (exactly one required)");
         }
         if (this.keyAliases.size() > 1) {
            throw new InvalidRequestException("Multiple PRIMARY KEYs specifed (exactly one required)");
         }
         if (stmt.hasCounters && params.defaultTimeToLive > 0) {
            throw new InvalidRequestException("Cannot set default_time_to_live on a table with counters");
         }
         List<ColumnIdentifier> kAliases = this.keyAliases.get(0);
         stmt.keyTypes = new ArrayList(kAliases.size());
         for (ColumnIdentifier alias : kAliases) {
            stmt.keyAliases.add(alias);
            AbstractType<?> t = this.getTypeAndRemove(stmt.columns, alias);
            if (t.asCQL3Type().getType() instanceof CounterColumnType) {
               throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", alias));
            }
            if (t.asCQL3Type().getType().referencesDuration()) {
               throw new InvalidRequestException(String.format("duration type is not supported for PRIMARY KEY part %s", alias));
            }
            if (this.staticColumns.contains(alias)) {
               throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", alias));
            }
            stmt.keyTypes.add(t);
         }
         stmt.clusteringTypes = new ArrayList(this.columnAliases.size());
         for (ColumnIdentifier t : this.columnAliases) {
            stmt.columnAliases.add(t);
            AbstractType<?> type = this.getTypeAndRemove(stmt.columns, t);
            if (type.asCQL3Type().getType() instanceof CounterColumnType) {
               throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", t));
            }
            if (type.asCQL3Type().getType().referencesDuration()) {
               throw new InvalidRequestException(String.format("duration type is not supported for PRIMARY KEY part %s", t));
            }
            if (this.staticColumns.contains(t)) {
               throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", t));
            }
            stmt.clusteringTypes.add(type);
         }
         if (stmt.hasCounters) {
            for (AbstractType type : stmt.columns.values()) {
               if (type.isCounter()) continue;
               throw new InvalidRequestException("Cannot mix counter and non counter columns in the same table");
            }
         }
         stmt.isDense = (useCompactStorage = this.properties.useCompactStorage) && !stmt.clusteringTypes.isEmpty();
         stmt.isCompound = !useCompactStorage || stmt.clusteringTypes.size() > 1;
         if (useCompactStorage) {
            if (!stmt.multicellColumns.isEmpty()) {
               throw new InvalidRequestException("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
            }
            if (!this.staticColumns.isEmpty()) {
               throw new InvalidRequestException("Static columns are not supported in COMPACT STORAGE tables");
            }
            if (stmt.clusteringTypes.isEmpty() && stmt.columns.isEmpty()) {
               throw new InvalidRequestException("No definition found that is not part of the PRIMARY KEY");
            }
            if (stmt.isDense) {
               if (stmt.columns.size() > 1) {
                  throw new InvalidRequestException(String.format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: %s)", StringUtils.join(stmt.columns.keySet(), (String)", ")));
               }
            } else if (stmt.columns.isEmpty()) {
               throw new InvalidRequestException("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");
            }
         } else if (stmt.clusteringTypes.isEmpty() && !this.staticColumns.isEmpty() && this.columnAliases.isEmpty()) {
            throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
         }
         if (!this.properties.definedOrdering.isEmpty()) {
            if (this.properties.definedOrdering.size() > this.columnAliases.size()) {
               throw new InvalidRequestException("Only clustering key columns can be defined in CLUSTERING ORDER directive");
            }
            int i = 0;
            for (ColumnIdentifier id : this.properties.definedOrdering.keySet()) {
               ColumnIdentifier c;
               if (!id.equals(c = this.columnAliases.get(i))) {
                  if (this.properties.definedOrdering.containsKey(c)) {
                     throw new InvalidRequestException(String.format("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key (%s must appear before %s)", c, id));
                  }
                  throw new InvalidRequestException(String.format("Missing CLUSTERING ORDER for column %s", c));
               }
               ++i;
            }
         }
         return new ParsedStatement.Prepared(stmt);
      }

      private AbstractType<?> getTypeAndRemove(Map<ColumnIdentifier, AbstractType> columns, ColumnIdentifier t) throws InvalidRequestException {
         AbstractType type = (AbstractType)columns.get(t);
         if(type == null) {
            throw new InvalidRequestException(String.format("Unknown definition %s referenced in PRIMARY KEY", new Object[]{t}));
         } else if(type.isMultiCell()) {
            if(type.isCollection()) {
               throw new InvalidRequestException(String.format("Invalid non-frozen collection type for PRIMARY KEY component %s", new Object[]{t}));
            } else {
               throw new InvalidRequestException(String.format("Invalid non-frozen user-defined type for PRIMARY KEY component %s", new Object[]{t}));
            }
         } else {
            columns.remove(t);
            Boolean isReversed = (Boolean)this.properties.definedOrdering.get(t);
            return (AbstractType)(isReversed != null && isReversed.booleanValue()?ReversedType.getInstance(type):type);
         }
      }

      public void addDefinition(ColumnIdentifier def, CQL3Type.Raw type, boolean isStatic) {
         this.definedNames.add(def);
         this.definitions.put(def, type);
         if(isStatic) {
            this.staticColumns.add(def);
         }

      }

      public void addKeyAliases(List<ColumnIdentifier> aliases) {
         this.keyAliases.add(aliases);
      }

      public void addColumnAlias(ColumnIdentifier alias) {
         this.columnAliases.add(alias);
      }
   }
}
