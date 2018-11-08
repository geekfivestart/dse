package com.datastax.bdp.db.audit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.RoleResource;
import org.apache.commons.lang3.StringUtils;

final class AuditFilters {
   public static IAuditFilter includeRoles(List<RoleResource> roles) {
      return (event) -> {
         Iterator var2 = roles.iterator();

         RoleResource role;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            role = (RoleResource)var2.next();
         } while(!event.userHasRole(role));

         return true;
      };
   }

   public static IAuditFilter excludeRoles(List<RoleResource> roles) {
      return not(includeRoles(roles));
   }

   public static <T> Set<T> toSet(T[] elements) {
      return (Set)Arrays.stream(elements).collect(Collectors.toSet());
   }

   public static IAuditFilter includeKeyspaces(List<Pattern> patterns) {
      return (event) -> {
         String keyspace = event.getKeyspace();
         if(keyspace == null) {
            return false;
         } else {
            Iterator var3 = patterns.iterator();

            Pattern pattern;
            do {
               if(!var3.hasNext()) {
                  return false;
               }

               pattern = (Pattern)var3.next();
            } while(!pattern.matcher(keyspace).matches());

            return true;
         }
      };
   }

   public static IAuditFilter excludeKeyspaces(String... patterns) {
      return excludeKeyspaces(toPatterns(patterns));
   }

   public static IAuditFilter excludeKeyspaces(List<Pattern> patterns) {
      return not(includeKeyspaces(patterns));
   }

   public static IAuditFilter includeCategories(Set<AuditableEventCategory> categories) {
      return (event) -> {
         return categories.contains(event.getType().getCategory());
      };
   }

   public static IAuditFilter excludeCategories(Set<AuditableEventCategory> categories) {
      return not(includeCategories(categories));
   }

   private static IAuditFilter not(IAuditFilter filter) {
      return (event) -> {
         return !filter.accept(event);
      };
   }

   public static IAuditFilter acceptEverything() {
      return (event) -> {
         return true;
      };
   }

   public static IAuditFilter composite(List<IAuditFilter> filters) {
      return (event) -> {
         Iterator var2 = filters.iterator();

         IAuditFilter filter;
         do {
            if(!var2.hasNext()) {
               return true;
            }

            filter = (IAuditFilter)var2.next();
         } while(filter.accept(event));

         return false;
      };
   }

   public static IAuditFilter fromConfiguration(AuditLoggingOptions options) {
      options.validateFilters();
      List<IAuditFilter> filters = new ArrayList();
      addCategoryFilters(options, filters);
      addKeyspaceFilters(options, filters);
      addRoleFilters(options, filters);
      return filters.isEmpty()?acceptEverything():(filters.size() == 1?(IAuditFilter)filters.get(0):composite(filters));
   }

   private static void addCategoryFilters(AuditLoggingOptions auditLoggingOptions, List<IAuditFilter> filters) {
      Set categories;
      if(!StringUtils.isBlank(auditLoggingOptions.included_categories)) {
         categories = toCategories(auditLoggingOptions.included_categories);
         if(!categories.isEmpty()) {
            filters.add(includeCategories(categories));
         }
      } else if(!StringUtils.isBlank(auditLoggingOptions.excluded_categories)) {
         categories = toCategories(auditLoggingOptions.excluded_categories);
         if(!categories.isEmpty()) {
            filters.add(excludeCategories(categories));
         }
      }

   }

   private static void addRoleFilters(AuditLoggingOptions auditLoggingOptions, List<IAuditFilter> filters) {
      List roles;
      if(!StringUtils.isBlank(auditLoggingOptions.included_roles)) {
         roles = toRoles(auditLoggingOptions.included_roles);
         if(!roles.isEmpty()) {
            filters.add(includeRoles(roles));
         }
      } else if(!StringUtils.isBlank(auditLoggingOptions.excluded_roles)) {
         roles = toRoles(auditLoggingOptions.excluded_roles);
         if(!roles.isEmpty()) {
            filters.add(excludeRoles(roles));
         }
      }

   }

   private static void addKeyspaceFilters(AuditLoggingOptions auditLoggingOptions, List<IAuditFilter> filters) {
      List patterns;
      if(!StringUtils.isBlank(auditLoggingOptions.included_keyspaces)) {
         patterns = toPatterns(auditLoggingOptions.included_keyspaces);
         if(!patterns.isEmpty()) {
            filters.add(includeKeyspaces(patterns));
         }
      } else if(!StringUtils.isBlank(auditLoggingOptions.excluded_keyspaces)) {
         patterns = toPatterns(auditLoggingOptions.excluded_keyspaces);
         if(!patterns.isEmpty()) {
            filters.add(excludeKeyspaces(patterns));
         }
      }

   }

   private static Set<AuditableEventCategory> toCategories(String categoriesAsString) {
      return (Set)Arrays.stream(categoriesAsString.split(",")).map(String::trim).filter((s) -> {
         return !s.isEmpty();
      }).map(AuditableEventCategory::fromString).collect(Collectors.toCollection(() -> {
         return EnumSet.noneOf(AuditableEventCategory.class);
      }));
   }

   private static List<RoleResource> toRoles(String rolesAsString) {
      return (List)Arrays.stream(rolesAsString.split(",")).map(String::trim).filter((s) -> {
         return !s.isEmpty();
      }).map(RoleResource::role).collect(Collectors.toList());
   }

   private static List<Pattern> toPatterns(String patternsAsString) {
      return toPatterns(patternsAsString.split(","));
   }

   private static List<Pattern> toPatterns(String[] patterns) {
      return (List)Arrays.stream(patterns).map(String::trim).filter((s) -> {
         return !s.isEmpty();
      }).map(Pattern::compile).collect(Collectors.toList());
   }

   private AuditFilters() {
   }
}
