package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.collect.UnmodifiableIterator;
import io.reactivex.Maybe;
import java.util.Iterator;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class DropTypeStatement extends SchemaAlteringStatement {
   private final UTName name;
   private final boolean ifExists;

   public DropTypeStatement(UTName name, boolean ifExists) {
      this.name = name;
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_TYPE;
   }

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(!this.name.hasKeyspace()) {
         this.name.setKeyspace(state.getKeyspace());
      }

   }

   public void checkAccess(QueryState state) {
      state.checkKeyspacePermission(this.keyspace(), CorePermission.DROP);
   }

   public void validate(QueryState state) throws RequestValidationException {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.name.getKeyspace());
      if(ksm == null) {
         if(!this.ifExists) {
            throw new InvalidRequestException(String.format("Cannot drop type in unknown keyspace %s", new Object[]{this.name.getKeyspace()}));
         }
      } else if(!ksm.types.get(this.name.getUserTypeName()).isPresent()) {
         if(!this.ifExists) {
            throw new InvalidRequestException(String.format("No user type named %s exists.", new Object[]{this.name}));
         }
      } else {
         Iterator var3 = ksm.functions.iterator();

         while(var3.hasNext()) {
            Function function = (Function)var3.next();
            if(function.returnType().referencesUserType(this.name.getStringTypeName())) {
               throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by function %s", new Object[]{this.name, function}));
            }

            Iterator var5 = function.argTypes().iterator();

            while(var5.hasNext()) {
               AbstractType<?> argType = (AbstractType)var5.next();
               if(argType.referencesUserType(this.name.getStringTypeName())) {
                  throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by function %s", new Object[]{this.name, function}));
               }
            }
         }

         var3 = ksm.types.iterator();

         while(var3.hasNext()) {
            UserType ut = (UserType)var3.next();
            if(!ut.name.equals(this.name.getUserTypeName()) && ut.referencesUserType(this.name.getStringTypeName())) {
               throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by user type %s", new Object[]{this.name, ut.getNameAsString()}));
            }
         }

         var3 = ksm.tablesAndViews().iterator();

         while(var3.hasNext()) {
            TableMetadata table = (TableMetadata)var3.next();
            UnmodifiableIterator var9 = table.columns().iterator();

            while(var9.hasNext()) {
               ColumnMetadata def = (ColumnMetadata)var9.next();
               if(def.type.referencesUserType(this.name.getStringTypeName())) {
                  throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by table %s", new Object[]{this.name, table.toString()}));
               }
            }
         }

      }
   }

   public String keyspace() {
      return this.name.getKeyspace();
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.name.getKeyspace());
      if(ksm == null) {
         return Maybe.empty();
      } else {
         UserType toDrop = ksm.types.getNullable(this.name.getUserTypeName());
         return toDrop == null?Maybe.empty():MigrationManager.announceTypeDrop(toDrop, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, this.keyspace(), this.name.getStringTypeName())));
      }
   }
}
