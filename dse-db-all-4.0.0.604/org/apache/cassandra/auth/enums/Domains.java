package org.apache.cassandra.auth.enums;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableSet.Builder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import org.github.jamm.Unmetered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Unmetered
public class Domains<E extends PartitionedEnum> {
   private static final Logger logger = LoggerFactory.getLogger(Domains.class);
   private static final Domains.GlobalTypeRegistry TYPE_REGISTRY = new Domains.GlobalTypeRegistry();
   private static final char DELIM = '.';
   private final Class<E> type;
   private volatile Map<String, Domains.Domain> domains;
   private int bitOffset;

   static <E extends PartitionedEnum, D extends Enum & PartitionedEnum> void registerDomain(Class<E> type, String domainName, Class<D> domain) {
      TYPE_REGISTRY.registerDomain(type, domainName, domain);
   }

   public static <E extends PartitionedEnum> Domains<E> getDomains(Class<E> type) {
      Domains<E> domains = TYPE_REGISTRY.getDomains(type);
      if(domains == null) {
         throw new IllegalArgumentException(String.format("No domains registered for partitioned enum class %s", new Object[]{type.getName()}));
      } else {
         return domains;
      }
   }

   @VisibleForTesting
   static <E extends PartitionedEnum> void unregisterType(Class<E> type) {
      TYPE_REGISTRY.unregister(type);
   }

   Collection<Domains.Domain> domains() {
      return this.domains.values();
   }

   Domains.Domain domain(String domain) {
      return (Domains.Domain)this.domains.get(domain);
   }

   private Domains(Class<E> type) {
      this.domains = new HashMap();
      this.type = type;
   }

   @VisibleForTesting
   <D extends Enum & PartitionedEnum> void register(String domain, Class<D> enumType) {
      if(domain.indexOf(46) != -1) {
         throw new IllegalArgumentException(String.format("Invalid domain %s, name must not include period", new Object[]{domain}));
      } else {
         domain = domain.toUpperCase(Locale.US);
         logger.trace("Registering domain {}", domain);
         if(!this.type.isAssignableFrom(enumType)) {
            throw new IllegalArgumentException(String.format("Supplied domain class %s is not a valid domain of enumerated type %s", new Object[]{enumType.getName(), this.type.getName()}));
         } else {
            Enum[] var3 = (Enum[])enumType.getEnumConstants();
            int var4 = var3.length;

            for(int var5 = 0; var5 < var4; ++var5) {
               Enum element = var3[var5];
               PartitionedEnum domainElement = (PartitionedEnum)this.type.cast(element);
               if(!domainElement.domain().toUpperCase(Locale.US).equals(domain)) {
                  throw new IllegalArgumentException(String.format("Invalid domain %s in enumerated type declaration %s.", new Object[]{domainElement.domain(), enumType.getClass().getName()}));
               }

               if(!domainElement.name().toUpperCase(Locale.US).equals(domainElement.name())) {
                  throw new IllegalArgumentException(String.format("Invalid name %s for %s, only upper case names are permitted", new Object[]{domainElement.getFullName(), this.type.getName()}));
               }
            }

            synchronized(this) {
               if(this.domains.containsKey(domain)) {
                  throw new IllegalArgumentException(String.format("Domain %s was already registered for type %s", new Object[]{domain, this.type.getName()}));
               } else {
                  Map<String, Domains.Domain> domainsUpdate = new HashMap(this.domains);
                  D[] enumConstants = (Enum[])enumType.getEnumConstants();
                  Domains.Domain d = new Domains.Domain(domainsUpdate.size(), this.bitOffset, enumType);
                  this.bitOffset += enumConstants.length;
                  domainsUpdate.put(domain, d);
                  this.domains = domainsUpdate;
               }
            }
         }
      }
   }

   public Class<E> getType() {
      return this.type;
   }

   public E get(String domain, String name) {
      domain = domain.toUpperCase(Locale.US);
      name = name.toUpperCase(Locale.US);
      Domains.Domain d = (Domains.Domain)this.domains.get(domain);
      if(d == null) {
         throw new IllegalArgumentException(String.format("Unknown domain %s", new Object[]{domain}));
      } else {
         return (PartitionedEnum)Enum.valueOf(d.enumType, name);
      }
   }

   public E get(String fullName) {
      int delim = fullName.indexOf(46);

      assert delim > 0;

      String domain = fullName.substring(0, delim);
      String name = fullName.substring(delim + 1);
      return this.get(domain, name);
   }

   public ImmutableSet<E> asSet() {
      Builder<E> builder = ImmutableSet.builder();
      this.domains.values().forEach((d) -> {
         builder.add((PartitionedEnum[])((PartitionedEnum[])d.enumType.getEnumConstants()));
      });
      return builder.build();
   }

   @VisibleForTesting
   public void clear() {
      this.domains.clear();
   }

   private static final class GlobalTypeRegistry {
      private final Map<Class<?>, Domains<?>> KNOWN_TYPES;

      private GlobalTypeRegistry() {
         this.KNOWN_TYPES = Maps.newConcurrentMap();
      }

      private <E extends PartitionedEnum> Domains<E> getDomains(Class<E> type) {
         Domains<?> domains = (Domains)this.KNOWN_TYPES.get(type);
         if(domains == null) {
            throw new IllegalArgumentException("Unknown PartitionedEnumType " + type.getName());
         } else {
            return domains;
         }
      }

      private <E extends PartitionedEnum, D extends Enum & PartitionedEnum> void registerDomain(Class<E> type, String domainName, Class<D> domain) {
         Domains<E> newRegistry = new Domains(type);
         Domains<?> existingRegistry = (Domains)this.KNOWN_TYPES.putIfAbsent(type, newRegistry);
         Domains<?> registry = existingRegistry != null?existingRegistry:newRegistry;
         registry.register(domainName, domain);
      }

      private <E extends PartitionedEnum> void unregister(Class<E> type) {
         this.KNOWN_TYPES.remove(type);
      }
   }

   static final class Domain {
      final int ordinal;
      final int bitOffset;
      final Class<? extends Enum> enumType;

      private Domain(int ordinal, int bitOffset, Class<? extends Enum> enumType) {
         this.ordinal = ordinal;
         this.bitOffset = bitOffset;
         this.enumType = enumType;
      }
   }
}
