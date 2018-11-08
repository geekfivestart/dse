package org.apache.cassandra.cql3.functions;

import java.util.Objects;
import org.apache.cassandra.cql3.ColumnIdentifier;

public final class FunctionName {
   private static final FunctionName TOKEN_FUNCTION_NAME = nativeFunction("token");
   public final String keyspace;
   public final String name;

   public static FunctionName nativeFunction(String name) {
      return new FunctionName("system", name);
   }

   public FunctionName(String keyspace, String name) {
      assert name != null : "Name parameter must not be null";

      this.keyspace = keyspace;
      this.name = name;
   }

   public FunctionName asNativeFunction() {
      return nativeFunction(this.name);
   }

   public boolean hasKeyspace() {
      return this.keyspace != null;
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{this.keyspace, this.name});
   }

   public final boolean equals(Object o) {
      if(!(o instanceof FunctionName)) {
         return false;
      } else {
         FunctionName that = (FunctionName)o;
         return Objects.equals(this.keyspace, that.keyspace) && Objects.equals(this.name, that.name);
      }
   }

   public final boolean equalsNativeFunction(FunctionName nativeFunction) {
      assert nativeFunction.keyspace.equals("system");

      return this.hasKeyspace() && !this.keyspace.equals("system")?false:Objects.equals(this.name, nativeFunction.name);
   }

   public String toString() {
      return this.keyspace == null?this.name:this.keyspace + '.' + this.name;
   }

   public String toCQLString() {
      String maybeQuotedName = this.equalsNativeFunction(TOKEN_FUNCTION_NAME)?this.name:ColumnIdentifier.maybeQuote(this.name);
      return this.keyspace == null?maybeQuotedName:ColumnIdentifier.maybeQuote(this.keyspace) + '.' + maybeQuotedName;
   }
}
