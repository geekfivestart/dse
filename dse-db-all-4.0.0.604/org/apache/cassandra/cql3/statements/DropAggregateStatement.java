package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.AggregateFunction;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public final class DropAggregateStatement extends SchemaAlteringStatement {
   private FunctionName functionName;
   private final boolean ifExists;
   private final List<CQL3Type.Raw> argRawTypes;
   private final boolean argsPresent;

   public DropAggregateStatement(FunctionName functionName, List<CQL3Type.Raw> argRawTypes, boolean argsPresent, boolean ifExists) {
      this.functionName = functionName;
      this.argRawTypes = argRawTypes;
      this.argsPresent = argsPresent;
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_AGGREGATE;
   }

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(!this.functionName.hasKeyspace() && state.getRawKeyspace() != null) {
         this.functionName = new FunctionName(state.getKeyspace(), this.functionName.name);
      }

      if(!this.functionName.hasKeyspace()) {
         throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");
      } else {
         Schema.validateKeyspaceNotSystem(this.functionName.keyspace);
      }
   }

   public String keyspace() {
      return this.functionName.keyspace;
   }

   public void checkAccess(QueryState state) {
      state.checkKeyspacePermission(this.functionName.keyspace, CorePermission.DROP);
   }

   public void validate(QueryState state) throws RequestValidationException {
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      Collection<Function> olds = Schema.instance.getFunctions(this.functionName);
      if(!this.argsPresent && olds != null && olds.size() > 1) {
         return this.error(String.format("'DROP AGGREGATE %s' matches multiple function definitions; specify the argument types by issuing a statement like 'DROP AGGREGATE %s (type, type, ...)'. Hint: use cqlsh 'DESCRIBE AGGREGATE %s' command to find all overloads", new Object[]{this.functionName, this.functionName, this.functionName}));
      } else {
         Function old = null;
         if(!this.argsPresent) {
            if(olds == null || olds.isEmpty() || !(olds.iterator().next() instanceof AggregateFunction)) {
               if(this.ifExists) {
                  return Maybe.empty();
               }

               return this.error(String.format("Cannot drop non existing aggregate '%s'", new Object[]{this.functionName}));
            }

            old = (Function)olds.iterator().next();
         } else {
            Iterator var6;
            CQL3Type.Raw rawType;
            if(Schema.instance.getKeyspaceMetadata(this.functionName.keyspace) != null) {
               List<AbstractType<?>> argTypes = new ArrayList(this.argRawTypes.size());
               var6 = this.argRawTypes.iterator();

               while(var6.hasNext()) {
                  rawType = (CQL3Type.Raw)var6.next();
                  argTypes.add(this.prepareType("arguments", rawType));
               }

               old = (Function)Schema.instance.findFunction(this.functionName, argTypes).orElse((Object)null);
            }

            if(old == null || !(old instanceof AggregateFunction)) {
               if(this.ifExists) {
                  return Maybe.empty();
               } else {
                  StringBuilder sb = new StringBuilder();

                  for(var6 = this.argRawTypes.iterator(); var6.hasNext(); sb.append(rawType)) {
                     rawType = (CQL3Type.Raw)var6.next();
                     if(sb.length() > 0) {
                        sb.append(", ");
                     }
                  }

                  return this.error(String.format("Cannot drop non existing aggregate '%s(%s)'", new Object[]{this.functionName, sb}));
               }
            }
         }

         if(old.isNative()) {
            return this.error(String.format("Cannot drop aggregate '%s' because it is a native (built-in) function", new Object[]{this.functionName}));
         } else {
            return MigrationManager.announceAggregateDrop((UDAggregate)old, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.AGGREGATE, old.name().keyspace, old.name().name, AbstractType.asCQLTypeStringList(old.argTypes()))));
         }
      }
   }

   private AbstractType<?> prepareType(String typeName, CQL3Type.Raw rawType) {
      if(rawType.isFrozen()) {
         throw new InvalidRequestException(String.format("The function %s should not be frozen; remove the frozen<> modifier", new Object[]{typeName}));
      } else {
         if(!rawType.canBeNonFrozen()) {
            rawType.freeze();
         }

         AbstractType<?> type = rawType.prepare(this.functionName.keyspace).getType();
         return type;
      }
   }
}
