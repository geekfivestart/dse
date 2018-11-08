package org.apache.cassandra.utils.versioning;

public abstract class VersionDependent<V extends Enum<V> & Version<V>> {
   protected final V version;

   protected VersionDependent(V version) {
      this.version = version;
   }
}
