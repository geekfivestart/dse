package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class CreateTypeStatement extends SchemaAlteringStatement {
   private final UTName name;
   private final List<FieldIdentifier> columnNames = new ArrayList();
   private final List<CQL3Type.Raw> columnTypes = new ArrayList();
   private final boolean ifNotExists;

   public CreateTypeStatement(UTName name, boolean ifNotExists) {
      this.name = name;
      this.ifNotExists = ifNotExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_TYPE;
   }

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(!this.name.hasKeyspace()) {
         this.name.setKeyspace(state.getKeyspace());
      }

   }

   public static UserType parse(String cql, String keyspace) {
      return ((CreateTypeStatement)CQLFragmentParser.parseAny(CqlParser::createTypeStatement, cql, "CREATE TYPE")).createType(keyspace, Types.none());
   }

   public void addDefinition(FieldIdentifier name, CQL3Type.Raw type) {
      this.columnNames.add(name);
      this.columnTypes.add(type);
   }

   public void checkAccess(QueryState state) {
      state.checkKeyspacePermission(this.keyspace(), CorePermission.CREATE);
   }

   public void validate(QueryState state) throws RequestValidationException {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.name.getKeyspace());
      if(ksm == null) {
         throw new InvalidRequestException(String.format("Cannot add type in unknown keyspace %s", new Object[]{this.name.getKeyspace()}));
      } else if(ksm.types.get(this.name.getUserTypeName()).isPresent() && !this.ifNotExists) {
         throw new InvalidRequestException(String.format("A user type of name %s already exists", new Object[]{this.name}));
      } else {
         Iterator var3 = this.columnTypes.iterator();

         CQL3Type.Raw type;
         do {
            if(!var3.hasNext()) {
               return;
            }

            type = (CQL3Type.Raw)var3.next();
            if(type.isCounter()) {
               throw new InvalidRequestException("A user type cannot contain counters");
            }
         } while(!type.isUDT() || type.isFrozen());

         throw new InvalidRequestException("A user type cannot contain non-frozen UDTs");
      }
   }

   public static String haveDuplicateName(UserType type) throws InvalidRequestException {
      for(int i = 0; i < type.size() - 1; ++i) {
         FieldIdentifier fieldName = type.fieldName(i);

         for(int j = i + 1; j < type.size(); ++j) {
            if(fieldName.equals(type.fieldName(j))) {
               return fieldName.toString();
            }
         }
      }

      return null;
   }

   public void addToRawBuilder(Types.RawBuilder builder) throws InvalidRequestException {
      builder.add(this.name.getStringTypeName(), (List)this.columnNames.stream().map(FieldIdentifier::toString).collect(Collectors.toList()), (List)this.columnTypes.stream().map(Object::toString).collect(Collectors.toList()));
   }

   public String keyspace() {
      return this.name.getKeyspace();
   }

   public UserType createType() throws InvalidRequestException {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.keyspace());
      if(ksm == null) {
         throw new ConfigurationException(String.format("Keyspace %s doesn't exist", new Object[]{this.keyspace()}));
      } else {
         return this.createType(this.keyspace(), ksm.types);
      }
   }

   public UserType createType(String keyspace, Types keyspaceTypes) throws InvalidRequestException {
      List<AbstractType<?>> types = new ArrayList(this.columnTypes.size());
      Iterator var4 = this.columnTypes.iterator();

      while(var4.hasNext()) {
         CQL3Type.Raw type = (CQL3Type.Raw)var4.next();
         types.add(type.prepare(keyspace, keyspaceTypes).getType());
      }

      return new UserType(keyspace, this.name.getUserTypeName(), this.columnNames, types, true);
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.name.getKeyspace());

      assert ksm != null;

      if(ksm.types.get(this.name.getUserTypeName()).isPresent()) {
         return Maybe.empty();
      } else {
         UserType type = this.createType();
         String duplicate = haveDuplicateName(type);
         return duplicate != null?this.error(String.format("Duplicate field name %s in type %s", new Object[]{duplicate, type.name})):MigrationManager.announceNewType(type, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, this.keyspace(), this.name.getStringTypeName())));
      }
   }
}
