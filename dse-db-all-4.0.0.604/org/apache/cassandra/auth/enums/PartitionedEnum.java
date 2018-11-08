package org.apache.cassandra.auth.enums;

public interface PartitionedEnum {
   String domain();

   String name();

   int ordinal();

   default String getFullName() {
      return this.domain() + '.' + this.name();
   }

   static default <E extends PartitionedEnum, D extends Enum & PartitionedEnum> void registerDomainForType(Class<E> type, String domainName, Class<D> domain) {
      Domains.registerDomain(type, domainName, domain);
   }
}
