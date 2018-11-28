package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.functions.AggregateFunction;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.ProtocolVersion;

public final class CreateAggregateStatement extends SchemaAlteringStatement {
   private final boolean orReplace;
   private final boolean ifNotExists;
   private final boolean deterministic;
   private FunctionName functionName;
   private FunctionName stateFunc;
   private FunctionName finalFunc;
   private final CQL3Type.Raw stateTypeRaw;
   private final List<CQL3Type.Raw> argRawTypes;
   private final Term.Raw ival;
   private List<AbstractType<?>> argTypes;
   private AbstractType<?> returnType;
   private ScalarFunction stateFunction;
   private ScalarFunction finalFunction;
   private ByteBuffer initcond;

   public CreateAggregateStatement(FunctionName functionName, List<CQL3Type.Raw> argRawTypes, String stateFunc, CQL3Type.Raw stateType, String finalFunc, Term.Raw ival, boolean orReplace, boolean ifNotExists, boolean deterministic) {
      this.functionName = functionName;
      this.argRawTypes = argRawTypes;
      this.stateFunc = new FunctionName(functionName.keyspace, stateFunc);
      this.finalFunc = finalFunc != null?new FunctionName(functionName.keyspace, finalFunc):null;
      this.stateTypeRaw = stateType;
      this.ival = ival;
      this.orReplace = orReplace;
      this.ifNotExists = ifNotExists;
      this.deterministic = deterministic;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_AGGREGATE;
   }

   public ParsedStatement.Prepared prepare() {
      this.argTypes = new ArrayList(this.argRawTypes.size());
      Iterator var1 = this.argRawTypes.iterator();

      while(var1.hasNext()) {
         CQL3Type.Raw rawType = (CQL3Type.Raw)var1.next();
         this.argTypes.add(this.prepareType("arguments", rawType));
      }

      AbstractType<?> stateType = this.prepareType("state type", this.stateTypeRaw);
      List<AbstractType<?>> stateArgs = stateArguments(stateType, this.argTypes);
      Function f = (Function)Schema.instance.findFunction(this.stateFunc, stateArgs).orElse(null);
      if(!(f instanceof ScalarFunction)) {
         throw new InvalidRequestException("State function " + stateFuncSig(this.stateFunc, this.stateTypeRaw, this.argRawTypes) + " does not exist or is not a scalar function");
      } else {
         this.stateFunction = (ScalarFunction)f;
         AbstractType<?> stateReturnType = this.stateFunction.returnType();
         if(!stateReturnType.equals(stateType)) {
            throw new InvalidRequestException("State function " + stateFuncSig(this.stateFunction.name(), this.stateTypeRaw, this.argRawTypes) + " return type must be the same as the first argument type - check STYPE, argument and return types");
         } else {
            if(this.finalFunc != null) {
               List<AbstractType<?>> finalArgs = Collections.singletonList(stateType);
               f = (Function)Schema.instance.findFunction(this.finalFunc, finalArgs).orElse(null);
               if(!(f instanceof ScalarFunction)) {
                  throw new InvalidRequestException("Final function " + this.finalFunc + '(' + this.stateTypeRaw + ") does not exist or is not a scalar function");
               }

               this.finalFunction = (ScalarFunction)f;
               this.returnType = this.finalFunction.returnType();
            } else {
               this.returnType = stateReturnType;
            }

            if(this.ival != null) {
               this.initcond = Terms.asBytes(this.functionName.keyspace, this.ival.toString(), stateType);
               if(this.initcond != null) {
                  try {
                     stateType.validate(this.initcond);
                  } catch (MarshalException var6) {
                     throw new InvalidRequestException(String.format("Invalid value for INITCOND of type %s%s", new Object[]{stateType.asCQL3Type(), var6.getMessage() == null?"":String.format(" (%s)", new Object[]{var6.getMessage()})}));
                  }
               }

               String initcondAsCql = stateType.asCQL3Type().toCQLLiteral(this.initcond, ProtocolVersion.CURRENT);

               assert Objects.equals(this.initcond, Terms.asBytes(this.functionName.keyspace, initcondAsCql, stateType));

               if(Constants.NULL_LITERAL != this.ival && isNullOrEmpty(stateType, this.initcond)) {
                  throw new InvalidRequestException("INITCOND must not be empty for all types except TEXT, ASCII, BLOB");
               }
            }

            return super.prepare();
         }
      }
   }

   public static boolean isNullOrEmpty(AbstractType<?> type, ByteBuffer bb) {
      return bb == null || bb.remaining() == 0 && type.isEmptyValueMeaningless();
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

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(!this.functionName.hasKeyspace() && state.getRawKeyspace() != null) {
         this.functionName = new FunctionName(state.getKeyspace(), this.functionName.name);
      }

      if(!this.functionName.hasKeyspace()) {
         throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");
      } else {
         Schema.validateKeyspaceNotSystem(this.functionName.keyspace);
         this.stateFunc = new FunctionName(this.functionName.keyspace, this.stateFunc.name);
         if(this.finalFunc != null) {
            this.finalFunc = new FunctionName(this.functionName.keyspace, this.finalFunc.name);
         }

      }
   }

   public String keyspace() {
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
      Optional<Function> existing = Schema.instance.findFunction(this.functionName, this.argTypes);
      if(existing.isPresent() && this.orReplace) {
         state.checkFunctionPermission((Function)((Function)existing.get()), CorePermission.ALTER);
      } else {
         state.checkFunctionPermission((FunctionResource)FunctionResource.keyspace(this.functionName.keyspace), CorePermission.CREATE);
      }

      state.checkFunctionPermission((Function)this.stateFunction, CorePermission.EXECUTE);
      if(this.finalFunction != null) {
         state.checkFunctionPermission((Function)this.finalFunction, CorePermission.EXECUTE);
      }

   }

   public void validate(QueryState state) throws InvalidRequestException {
      if(this.ifNotExists && this.orReplace) {
         throw new InvalidRequestException("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");
      } else if(Schema.instance.getKeyspaceMetadata(this.functionName.keyspace) == null) {
         throw new InvalidRequestException(String.format("Cannot add aggregate '%s' to non existing keyspace '%s'.", new Object[]{this.functionName.name, this.functionName.keyspace}));
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

         if(!(old instanceof AggregateFunction)) {
            return this.error(String.format("Aggregate %s can only replace an aggregate", new Object[]{old}));
         }

         if(old.isNative()) {
            return this.error(String.format("Cannot replace native aggregate %s", new Object[]{old}));
         }

         if(!old.returnType().isValueCompatibleWith(this.returnType)) {
            return this.error(String.format("Cannot replace aggregate %s, the new return type %s is not compatible with the return type %s of existing function", new Object[]{this.functionName, this.returnType.asCQL3Type(), old.returnType().asCQL3Type()}));
         }
      }

      if(!this.stateFunction.isCalledOnNullInput() && this.initcond == null) {
         return this.error(String.format("Cannot create aggregate %s without INITCOND because state function %s does not accept 'null' arguments", new Object[]{this.functionName, this.stateFunc}));
      } else {
         UDAggregate udAggregate = new UDAggregate(this.functionName, this.argTypes, this.returnType, this.stateFunction, this.finalFunction, this.initcond, this.deterministic);
         return MigrationManager.announceNewAggregate(udAggregate, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(replaced?Event.SchemaChange.Change.UPDATED:Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.AGGREGATE, udAggregate.name().keyspace, udAggregate.name().name, AbstractType.asCQLTypeStringList(udAggregate.argTypes()))));
      }
   }

   private static String stateFuncSig(FunctionName stateFuncName, CQL3Type.Raw stateTypeRaw, List<CQL3Type.Raw> argRawTypes) {
      StringBuilder sb = new StringBuilder();
      sb.append(stateFuncName.toString()).append('(').append(stateTypeRaw);
      Iterator var4 = argRawTypes.iterator();

      while(var4.hasNext()) {
         CQL3Type.Raw argRawType = (CQL3Type.Raw)var4.next();
         sb.append(", ").append(argRawType);
      }

      sb.append(')');
      return sb.toString();
   }

   private static List<AbstractType<?>> stateArguments(AbstractType<?> stateType, List<AbstractType<?>> argTypes) {
      List<AbstractType<?>> r = new ArrayList(argTypes.size() + 1);
      r.add(stateType);
      r.addAll(argTypes);
      return r;
   }
}
