package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.base.Joiner;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public final class DropFunctionStatement extends SchemaAlteringStatement {
   private FunctionName functionName;
   private final boolean ifExists;
   private final List<CQL3Type.Raw> argRawTypes;
   private final boolean argsPresent;
   private List<AbstractType<?>> argTypes;

   public DropFunctionStatement(FunctionName functionName, List<CQL3Type.Raw> argRawTypes, boolean argsPresent, boolean ifExists) {
      this.functionName = functionName;
      this.argRawTypes = argRawTypes;
      this.argsPresent = argsPresent;
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_FUNCTION;
   }

   public ParsedStatement.Prepared prepare() throws InvalidRequestException {
      if(Schema.instance.getKeyspaceMetadata(this.functionName.keyspace) != null) {
         this.argTypes = new ArrayList(this.argRawTypes.size());

         CQL3Type.Raw rawType;
         for(Iterator var1 = this.argRawTypes.iterator(); var1.hasNext(); this.argTypes.add(rawType.prepare(this.functionName.keyspace).getType())) {
            rawType = (CQL3Type.Raw)var1.next();
            if(rawType.isFrozen()) {
               throw new InvalidRequestException("The function arguments should not be frozen; remove the frozen<> modifier");
            }

            if(!rawType.canBeNonFrozen()) {
               rawType.freeze();
            }
         }
      }

      return super.prepare();
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
      assert this.functionName.hasKeyspace() : "The statement hasn't be prepared correctly";

      return this.functionName.keyspace;
   }

   public void checkAccess(QueryState state) {
      Function function = this.findFunction();
      if(function == null) {
         if(!this.ifExists) {
            throw new InvalidRequestException(String.format("Unconfigured function %s.%s(%s)", new Object[]{this.functionName.keyspace, this.functionName.name, Joiner.on(",").join(this.argRawTypes)}));
         }
      } else {
         state.checkFunctionPermission((Function)function, CorePermission.DROP);
      }

   }

   public void validate(QueryState state) {
      Collection<Function> olds = Schema.instance.getFunctions(this.functionName);
      if(!this.argsPresent && olds != null && olds.size() > 1) {
         throw new InvalidRequestException(String.format("'DROP FUNCTION %s' matches multiple function definitions; specify the argument types by issuing a statement like 'DROP FUNCTION %s (type, type, ...)'. Hint: use cqlsh 'DESCRIBE FUNCTION %s' command to find all overloads", new Object[]{this.functionName, this.functionName, this.functionName}));
      }
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      Function old = this.findFunction();
      if(old == null) {
         return this.ifExists?Maybe.empty():Maybe.error(new InvalidRequestException(this.getMissingFunctionError()));
      } else {
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(old.name().keyspace);
         Collection<UDAggregate> referrers = ksm.functions.aggregatesUsingFunction(old);
         return !referrers.isEmpty()?this.error(String.format("Function '%s' still referenced by %s", new Object[]{old, referrers})):MigrationManager.announceFunctionDrop((UDFunction)old, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.FUNCTION, old.name().keyspace, old.name().name, AbstractType.asCQLTypeStringList(old.argTypes()))));
      }
   }

   private String getMissingFunctionError() {
      StringBuilder sb = new StringBuilder("Cannot drop non existing function '");
      sb.append(this.functionName);
      if(this.argsPresent) {
         sb.append(Joiner.on(", ").join(this.argRawTypes));
      }

      sb.append('\'');
      return sb.toString();
   }

   private Function findFunction() {
      Function old;
      if(this.argsPresent) {
         if(this.argTypes == null) {
            return null;
         }

         old = (Function)Schema.instance.findFunction(this.functionName, this.argTypes).orElse((Object)null);
         if(old == null || !(old instanceof ScalarFunction)) {
            return null;
         }
      } else {
         Collection<Function> olds = Schema.instance.getFunctions(this.functionName);
         if(olds == null || olds.isEmpty() || !(olds.iterator().next() instanceof ScalarFunction)) {
            return null;
         }

         old = (Function)olds.iterator().next();
      }

      return old;
   }
}
