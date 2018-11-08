package org.apache.cassandra.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.UnmodifiableIterator;
import io.reactivex.Single;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import org.apache.cassandra.config.DatabaseDescriptor;

public class RolesCache extends AuthCache<RoleResource, Role> implements RolesCacheMBean {
   public RolesCache(IRoleManager roleManager) {
      this(roleManager::getRoleData, roleManager::getRolesData, () -> {
         return DatabaseDescriptor.getAuthenticator().requireAuthentication();
      });
   }

   @VisibleForTesting
   public RolesCache(Function<RoleResource, Role> loadFunction, Function<Iterable<? extends RoleResource>, Map<RoleResource, Role>> loadAllFunction, BooleanSupplier enabledFunction) {
      super("RolesCache", DatabaseDescriptor::setRolesValidity, DatabaseDescriptor::getRolesValidity, DatabaseDescriptor::setRolesUpdateInterval, DatabaseDescriptor::getRolesUpdateInterval, DatabaseDescriptor::setRolesCacheMaxEntries, DatabaseDescriptor::getRolesCacheMaxEntries, DatabaseDescriptor::setRolesCacheInitialCapacity, DatabaseDescriptor::getRolesCacheInitialCapacity, loadFunction, loadAllFunction, enabledFunction);
   }

   public Single<Map<RoleResource, Role>> getRoles(RoleResource primaryRole) {
      Single<Role> primary = this.get(primaryRole);
      return primary.flatMap((role) -> {
         if(role == null) {
            return Single.just(Collections.emptyMap());
         } else if(role.memberOf.isEmpty()) {
            return Single.just(Collections.singletonMap(primaryRole, role));
         } else {
            Map<RoleResource, Role> map = new HashMap();
            Set<RoleResource> remaining = new HashSet();
            map.put(primaryRole, role);
            remaining.addAll(role.memberOf);
            return this.collectRoles(Collections.synchronizedSet(remaining), Collections.synchronizedMap(map));
         }
      });
   }

   private Single<Map<RoleResource, Role>> collectRoles(Set<RoleResource> needToVisit, Map<RoleResource, Role> workMap) {
      if(!needToVisit.isEmpty()) {
         Map<RoleResource, Role> presentRoles = this.getAllPresent(needToVisit);
         this.handleCollectRoles(needToVisit, workMap, presentRoles);
         return this.getAll(needToVisit).flatMap((loadedRoles) -> {
            this.handleCollectRoles(needToVisit, workMap, loadedRoles);
            return this.collectRoles(needToVisit, workMap);
         });
      } else {
         return Single.just(Collections.unmodifiableMap(workMap));
      }
   }

   private void handleCollectRoles(Set<RoleResource> needToVisit, Map<RoleResource, Role> workMap, Map<RoleResource, Role> result) {
      Entry entry;
      for(Iterator var4 = result.entrySet().iterator(); var4.hasNext(); needToVisit.remove(entry.getKey())) {
         entry = (Entry)var4.next();
         Role role = (Role)entry.getValue();
         if(workMap.put(entry.getKey(), role) == null) {
            UnmodifiableIterator var7 = role.memberOf.iterator();

            while(var7.hasNext()) {
               RoleResource memberOf = (RoleResource)var7.next();
               if(!workMap.containsKey(memberOf)) {
                  needToVisit.add(memberOf);
               }
            }
         }
      }

   }
}
