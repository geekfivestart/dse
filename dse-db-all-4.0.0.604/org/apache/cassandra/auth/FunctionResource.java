package org.apache.cassandra.auth;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.commons.lang3.StringUtils;

public class FunctionResource implements IResource {
   private static final Set<Permission> COLLECTION_LEVEL_PERMISSIONS;
   private static final Set<Permission> SCALAR_FUNCTION_PERMISSIONS;
   private static final Set<Permission> AGGREGATE_FUNCTION_PERMISSIONS;
   private static final String ROOT_NAME = "functions";
   private static final FunctionResource ROOT_RESOURCE;
   private final FunctionResource.Level level;
   private final String keyspace;
   private final String name;
   private final List<AbstractType<?>> argTypes;

   private FunctionResource() {
      this.level = FunctionResource.Level.ROOT;
      this.keyspace = null;
      this.name = null;
      this.argTypes = null;
   }

   private FunctionResource(String keyspace) {
      this.level = FunctionResource.Level.KEYSPACE;
      this.keyspace = keyspace;
      this.name = null;
      this.argTypes = null;
   }

   private FunctionResource(String keyspace, String name, List<AbstractType<?>> argTypes) {
      this.level = FunctionResource.Level.FUNCTION;
      this.keyspace = keyspace;
      this.name = name;
      this.argTypes = argTypes;
   }

   public static FunctionResource root() {
      return ROOT_RESOURCE;
   }

   public static FunctionResource keyspace(String keyspace) {
      return new FunctionResource(keyspace);
   }

   public static FunctionResource function(String keyspace, String name, List<AbstractType<?>> argTypes) {
      return new FunctionResource(keyspace, name, argTypes);
   }

   public static FunctionResource functionFromCql(String keyspace, String name, List<CQL3Type.Raw> argTypes) {
      if(keyspace == null) {
         throw new InvalidRequestException("In this context function name must be explictly qualified by a keyspace");
      } else {
         List<AbstractType<?>> abstractTypes = new ArrayList(argTypes.size());
         Iterator var4 = argTypes.iterator();

         while(var4.hasNext()) {
            CQL3Type.Raw cqlType = (CQL3Type.Raw)var4.next();
            abstractTypes.add(cqlType.prepare(keyspace).getType());
         }

         return new FunctionResource(keyspace, name, abstractTypes);
      }
   }

   public static FunctionResource fromName(String name) {
      String[] parts = StringUtils.split(name, '/');
      if(parts[0].equals("functions") && parts.length <= 3) {
         if(parts.length == 1) {
            return root();
         } else {
            String ks = parts[1];
            if(parts.length == 2) {
               return keyspace(ks);
            } else {
               String[] nameAndArgs = StringUtils.split(parts[2], "[|]");
               String fName = nameAndArgs[0];
               List<AbstractType<?>> argTypeList = nameAndArgs.length > 1?argsListFromString(nameAndArgs[1]):Collections.emptyList();
               return function(ks, fName, argTypeList);
            }
         }
      } else {
         throw new IllegalArgumentException(String.format("%s is not a valid function resource name", new Object[]{name}));
      }
   }

   public String getName() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$FunctionResource$Level[this.level.ordinal()]) {
      case 1:
         return "functions";
      case 2:
         return String.format("%s/%s", new Object[]{"functions", this.keyspace});
      case 3:
         return String.format("%s/%s/%s[%s]", new Object[]{"functions", this.keyspace, this.name, this.argListAsString()});
      default:
         throw new AssertionError();
      }
   }

   public String getKeyspace() {
      return this.keyspace;
   }

   public FunctionName getFunctionName() {
      if(this.level != FunctionResource.Level.FUNCTION) {
         throw new IllegalStateException(String.format("%s function resource has no function name", new Object[]{this.level}));
      } else {
         return new FunctionName(this.keyspace, this.name);
      }
   }

   public IResource getParent() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$FunctionResource$Level[this.level.ordinal()]) {
      case 2:
         return root();
      case 3:
         return keyspace(this.keyspace);
      default:
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean hasParent() {
      return this.level != FunctionResource.Level.ROOT;
   }

   public boolean exists() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$FunctionResource$Level[this.level.ordinal()]) {
      case 1:
         return true;
      case 2:
         return Schema.instance.getKeyspaces().contains(this.keyspace);
      case 3:
         return Schema.instance.findFunction(this.getFunctionName(), this.argTypes).isPresent();
      default:
         throw new AssertionError();
      }
   }

   public Set<Permission> applicablePermissions() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$FunctionResource$Level[this.level.ordinal()]) {
      case 1:
      case 2:
         return COLLECTION_LEVEL_PERMISSIONS;
      case 3:
         Optional<Function> function = Schema.instance.findFunction(this.getFunctionName(), this.argTypes);

         assert function.isPresent() : "Unable to find function object for resource " + this.toString();

         return ((Function)function.get()).isAggregate()?AGGREGATE_FUNCTION_PERMISSIONS:SCALAR_FUNCTION_PERMISSIONS;
      default:
         throw new AssertionError();
      }
   }

   public int compareTo(FunctionResource o) {
      return this.name.compareTo(o.name);
   }

   public String toString() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$FunctionResource$Level[this.level.ordinal()]) {
      case 1:
         return "<all functions>";
      case 2:
         return String.format("<all functions in %s>", new Object[]{this.keyspace});
      case 3:
         return String.format("<function %s.%s(%s)>", new Object[]{this.keyspace, this.name, Joiner.on(", ").join(AbstractType.asCQLTypeStringList(this.argTypes))});
      default:
         throw new AssertionError();
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof FunctionResource)) {
         return false;
      } else {
         FunctionResource f = (FunctionResource)o;
         return Objects.equals(this.level, f.level) && Objects.equals(this.keyspace, f.keyspace) && Objects.equals(this.name, f.name) && Objects.equals(this.argTypes, f.argTypes);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.level, this.keyspace, this.name, this.argTypes});
   }

   private String argListAsString() {
      return Joiner.on("^").join(this.argTypes);
   }

   private static List<AbstractType<?>> argsListFromString(String s) {
      List<AbstractType<?>> argTypes = new ArrayList();
      Iterator var2 = Splitter.on("^").omitEmptyStrings().trimResults().split(s).iterator();

      while(var2.hasNext()) {
         String type = (String)var2.next();
         argTypes.add(TypeParser.parse(type));
      }

      return argTypes;
   }

   static {
      COLLECTION_LEVEL_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.CREATE, CorePermission.ALTER, CorePermission.DROP, CorePermission.AUTHORIZE, CorePermission.EXECUTE});
      SCALAR_FUNCTION_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.ALTER, CorePermission.DROP, CorePermission.AUTHORIZE, CorePermission.EXECUTE});
      AGGREGATE_FUNCTION_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.ALTER, CorePermission.DROP, CorePermission.AUTHORIZE, CorePermission.EXECUTE});
      ROOT_RESOURCE = new FunctionResource();
   }

   static enum Level {
      ROOT,
      KEYSPACE,
      FUNCTION;

      private Level() {
      }
   }
}
