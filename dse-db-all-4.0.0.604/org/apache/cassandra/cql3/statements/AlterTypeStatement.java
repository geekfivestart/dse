package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CQL3Type;
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
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public abstract class AlterTypeStatement extends SchemaAlteringStatement {
   protected final UTName name;

   protected AlterTypeStatement(UTName name) {
      this.name = name;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.UPDATE_TYPE;
   }

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(!this.name.hasKeyspace()) {
         this.name.setKeyspace(state.getKeyspace());
      }

      if(this.name.getKeyspace() == null) {
         throw new InvalidRequestException("You need to be logged in a keyspace or use a fully qualified user type name");
      }
   }

   protected abstract UserType makeUpdatedType(UserType var1, KeyspaceMetadata var2) throws InvalidRequestException;

   public static AlterTypeStatement addition(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type) {
      return new AlterTypeStatement.Add(name, fieldName, type);
   }

   public static AlterTypeStatement alter(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type) {
      throw new InvalidRequestException("Altering of types is not allowed");
   }

   public static AlterTypeStatement renames(UTName name, Map<FieldIdentifier, FieldIdentifier> renames) {
      return new AlterTypeStatement.Renames(name, renames);
   }

   public void checkAccess(QueryState state) {
      state.checkKeyspacePermission(this.keyspace(), CorePermission.ALTER);
   }

   public void validate(QueryState state) throws RequestValidationException {
      if(!SchemaConstants.isUserKeyspace(this.keyspace())) {
         throw new InvalidRequestException("Cannot ALTER types in system keyspace " + this.keyspace());
      }
   }

   public String keyspace() {
      return this.name.getKeyspace();
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.name.getKeyspace());
      if(ksm == null) {
         return this.error(String.format("Cannot alter type in unknown keyspace %s", new Object[]{this.name.getKeyspace()}));
      } else {
         UserType toUpdate = ksm.types.getNullable(this.name.getUserTypeName());
         if(toUpdate == null) {
            return this.error(String.format("No user type named %s exists.", new Object[]{this.name}));
         } else {
            UserType updated;
            try {
               updated = this.makeUpdatedType(toUpdate, ksm);
            } catch (InvalidRequestException var7) {
               return Maybe.error(var7);
            }

            List<Completable> migrations = new ArrayList();
            migrations.add(MigrationManager.announceTypeUpdate(updated, isLocalOnly));
            return Completable.concat(migrations).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, this.keyspace(), this.name.getStringTypeName())));
         }
      }
   }

   protected void checkTypeNotUsedByAggregate(KeyspaceMetadata ksm) {
      ksm.functions.udas().filter((aggregate) -> {
         return aggregate.initialCondition() != null && aggregate.stateType().referencesUserType(this.name.getStringTypeName());
      }).findAny().ifPresent((aggregate) -> {
         throw new InvalidRequestException(String.format("Cannot alter user type %s as it is still used as an INITCOND by aggregate %s", new Object[]{this.name, aggregate}));
      });
   }

   private static class Renames extends AlterTypeStatement {
      private final Map<FieldIdentifier, FieldIdentifier> renames;

      public Renames(UTName name, Map<FieldIdentifier, FieldIdentifier> renames) {
         super(name);
         this.renames = renames;
      }

      protected UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException {
         this.checkTypeNotUsedByAggregate(ksm);
         List<FieldIdentifier> newNames = new ArrayList(toUpdate.fieldNames());
         List<AbstractType<?>> newTypes = new ArrayList(toUpdate.fieldTypes());
         Iterator var5 = this.renames.entrySet().iterator();

         while(var5.hasNext()) {
            Entry<FieldIdentifier, FieldIdentifier> entry = (Entry)var5.next();
            FieldIdentifier from = (FieldIdentifier)entry.getKey();
            FieldIdentifier to = (FieldIdentifier)entry.getValue();
            int idx = toUpdate.fieldPosition(from);
            if(idx < 0) {
               throw new InvalidRequestException(String.format("Unknown field %s in type %s", new Object[]{from, this.name}));
            }

            newNames.set(idx, to);
         }

         UserType updated = new UserType(toUpdate.keyspace, toUpdate.name, newNames, newTypes, toUpdate.isMultiCell());
         String duplicate = CreateTypeStatement.haveDuplicateName(updated);
         if(duplicate != null) {
            throw new InvalidRequestException(String.format("Duplicate field name %s in type %s", new Object[]{duplicate, updated.name}));
         } else {
            return updated;
         }
      }
   }

   private static class Add extends AlterTypeStatement {
      private final FieldIdentifier fieldName;
      private final CQL3Type.Raw type;

      public Add(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type) {
         super(name);
         this.fieldName = fieldName;
         this.type = type;
      }

      protected UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException {
         if(toUpdate.fieldPosition(this.fieldName) >= 0) {
            throw new InvalidRequestException(String.format("Cannot add new field %s to type %s: a field of the same name already exists", new Object[]{this.fieldName, this.name}));
         } else {
            List<FieldIdentifier> newNames = new ArrayList(toUpdate.size() + 1);
            newNames.addAll(toUpdate.fieldNames());
            newNames.add(this.fieldName);
            AbstractType<?> addType = this.type.prepare(this.keyspace()).getType();
            if(addType.referencesUserType(toUpdate.getNameAsString())) {
               throw new InvalidRequestException(String.format("Cannot add new field %s of type %s to type %s as this would create a circular reference", new Object[]{this.fieldName, this.type, this.name}));
            } else {
               List<AbstractType<?>> newTypes = new ArrayList(toUpdate.size() + 1);
               newTypes.addAll(toUpdate.fieldTypes());
               newTypes.add(addType);
               return new UserType(toUpdate.keyspace, toUpdate.name, newNames, newTypes, toUpdate.isMultiCell());
            }
         }
      }
   }
}
