package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public final class CreateFunctionStatement extends SchemaAlteringStatement {
   private final boolean orReplace;
   private final boolean ifNotExists;
   private FunctionName functionName;
   private final String language;
   private final String body;
   private final List<ColumnIdentifier> argNames;
   private final List<CQL3Type.Raw> argRawTypes;
   private final CQL3Type.Raw rawReturnType;
   private final boolean calledOnNullInput;
   private List<AbstractType<?>> argTypes;
   private AbstractType<?> returnType;
   private final boolean deterministic;
   private final boolean monotonic;
   private final List<ColumnIdentifier> monotonicOn;

   public CreateFunctionStatement(FunctionName functionName, String language, String body, List<ColumnIdentifier> argNames, List<CQL3Type.Raw> argRawTypes, CQL3Type.Raw rawReturnType, boolean calledOnNullInput, boolean orReplace, boolean ifNotExists, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      this.functionName = functionName;
      this.language = language;
      this.body = body;
      this.argNames = argNames;
      this.argRawTypes = argRawTypes;
      this.rawReturnType = rawReturnType;
      this.calledOnNullInput = calledOnNullInput;
      this.orReplace = orReplace;
      this.ifNotExists = ifNotExists;
      this.deterministic = deterministic;
      this.monotonic = monotonic;
      this.monotonicOn = monotonicOn;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_FUNCTION;
   }

   public ParsedStatement.Prepared prepare() throws InvalidRequestException {
      if((new HashSet(this.argNames)).size() != this.argNames.size()) {
         throw new InvalidRequestException(String.format("duplicate argument names for given function %s with argument names %s", new Object[]{this.functionName, this.argNames}));
      } else {
         this.argTypes = new ArrayList(this.argRawTypes.size());
         Iterator var1 = this.argRawTypes.iterator();

         while(var1.hasNext()) {
            CQL3Type.Raw rawType = (CQL3Type.Raw)var1.next();
            this.argTypes.add(this.prepareType("arguments", rawType));
         }

         this.returnType = this.prepareType("return type", this.rawReturnType);
         return super.prepare();
      }
   }

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(!this.functionName.hasKeyspace() && state.getRawKeyspace() != null) {
         this.functionName = new FunctionName(state.getRawKeyspace(), this.functionName.name);
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

   protected void grantPermissionsToCreator(QueryState state) {
      try {
         IResource resource = FunctionResource.function(this.functionName.keyspace, this.functionName.name, this.argTypes);
         IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
         RoleResource role = RoleResource.role(state.getClientState().getUser().getName());
         authorizer.grant(AuthenticatedUser.SYSTEM_USER, authorizer.applicablePermissions(resource), resource, role, new GrantMode[]{GrantMode.GRANT});
      } catch (RequestExecutionException var5) {
         throw new RuntimeException(var5);
      }
   }

   public void checkAccess(QueryState state) {
      if(Schema.instance.findFunction(this.functionName, this.argTypes).isPresent() && this.orReplace) {
         state.checkFunctionPermission((FunctionResource)FunctionResource.function(this.functionName.keyspace, this.functionName.name, this.argTypes), CorePermission.ALTER);
      } else {
         state.checkFunctionPermission((FunctionResource)FunctionResource.keyspace(this.functionName.keyspace), CorePermission.CREATE);
      }

   }

   public void validate(QueryState state) throws InvalidRequestException {
      UDFunction.assertUdfsEnabled(this.language);
      if(this.ifNotExists && this.orReplace) {
         throw new InvalidRequestException("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");
      } else if(Schema.instance.getKeyspaceMetadata(this.functionName.keyspace) == null) {
         throw new InvalidRequestException(String.format("Cannot add function '%s' to non existing keyspace '%s'.", new Object[]{this.functionName.name, this.functionName.keyspace}));
      } else if(!this.argNames.containsAll(this.monotonicOn)) {
         throw new InvalidRequestException(String.format("Monotony should be declared on one of the arguments, '%s' is not an argument", new Object[]{this.monotonicOn.get(0)}));
      }
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      Function old = (Function)Schema.instance.findFunction(this.functionName, this.argTypes).orElse(null);
      boolean replaced = old != null;
      if(replaced) {
         if(this.ifNotExists) {
            return Maybe.empty();
         }

         if(!this.orReplace) {
            return this.error(String.format("Function %s already exists", new Object[]{old}));
         }

         if(!(old instanceof ScalarFunction)) {
            return this.error(String.format("Function %s can only replace a function", new Object[]{old}));
         }

         if(this.calledOnNullInput != ((ScalarFunction)old).isCalledOnNullInput()) {
            return this.error(String.format("Function %s can only be replaced with %s", new Object[]{old, this.calledOnNullInput?"CALLED ON NULL INPUT":"RETURNS NULL ON NULL INPUT"}));
         }

         if(!Functions.typesMatch(old.returnType(), this.returnType)) {
            return this.error(String.format("Cannot replace function %s, the new return type %s is not compatible with the return type %s of existing function", new Object[]{this.functionName, this.returnType.asCQL3Type(), old.returnType().asCQL3Type()}));
         }
      }

      UDFunction udFunction = UDFunction.create(this.functionName, this.argNames, this.argTypes, this.returnType, this.calledOnNullInput, this.language, this.body, this.deterministic, this.monotonic, this.monotonicOn);
      return MigrationManager.announceNewFunction(udFunction, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(replaced?Event.SchemaChange.Change.UPDATED:Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION, udFunction.name().keyspace, udFunction.name().name, AbstractType.asCQLTypeStringList(udFunction.argTypes()))));
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
