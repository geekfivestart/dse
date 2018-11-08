package org.apache.cassandra.auth;

import java.util.Set;
import java.util.function.Supplier;

public interface IResource {
   String getName();

   IResource getParent();

   boolean hasParent();

   boolean exists();

   Set<Permission> applicablePermissions();

   default IResource qualifyWithKeyspace(Supplier<String> keyspace) {
      return this;
   }
}
