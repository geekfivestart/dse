package com.datastax.bdp.cassandra.auth;

import com.google.common.base.Objects;
import com.google.inject.Inject;
import java.util.Optional;
import java.util.Set;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.commons.lang3.StringUtils;

public class AuthenticationSchemeResource implements IResource {
   private static final Set<Permission> PERMISSIONS;
   private static final String ROOT_NAME = "authentication_schemes";
   private static final AuthenticationSchemeResource ROOT_RESOURCE;
   private final AuthenticationSchemeResource.Level level;
   private final AuthenticationScheme scheme;

   @Inject
   private AuthenticationSchemeResource() {
      this.level = AuthenticationSchemeResource.Level.ROOT;
      this.scheme = null;
   }

   private AuthenticationSchemeResource(AuthenticationScheme scheme) {
      this.level = AuthenticationSchemeResource.Level.SCHEME;
      this.scheme = scheme;
   }

   public static AuthenticationSchemeResource root() {
      return ROOT_RESOURCE;
   }

   public static AuthenticationSchemeResource scheme(AuthenticationScheme scheme) {
      return new AuthenticationSchemeResource(scheme);
   }

   public String getName() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$AuthenticationSchemeResource$Level[this.level.ordinal()]) {
      case 1:
         return "authentication_schemes";
      case 2:
         return String.format("%s/%s", new Object[]{"authentication_schemes", this.scheme});
      default:
         throw new AssertionError();
      }
   }

   public IResource getParent() {
      if(this.level == AuthenticationSchemeResource.Level.SCHEME) {
         return root();
      } else {
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean hasParent() {
      return this.level != AuthenticationSchemeResource.Level.ROOT;
   }

   public boolean exists() {
      return this.level == AuthenticationSchemeResource.Level.ROOT || this.scheme != null;
   }

   public Set<Permission> applicablePermissions() {
      return PERMISSIONS;
   }

   public String toString() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$AuthenticationSchemeResource$Level[this.level.ordinal()]) {
      case 1:
         return "<all schemes>";
      case 2:
         return String.format("<scheme %s>", new Object[]{this.scheme});
      default:
         throw new AssertionError();
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof AuthenticationSchemeResource)) {
         return false;
      } else {
         AuthenticationSchemeResource rs = (AuthenticationSchemeResource)o;
         return Objects.equal(this.level, rs.level) && Objects.equal(this.scheme, rs.scheme);
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.level, this.scheme});
   }

   static {
      PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.EXECUTE, CorePermission.AUTHORIZE});
      ROOT_RESOURCE = new AuthenticationSchemeResource();
   }

   public static class Factory implements DseResourceFactory.Factory {
      public Factory() {
      }

      public boolean matches(String name) {
         return name.startsWith("authentication_schemes");
      }

      public AuthenticationSchemeResource fromName(String name) {
         String[] parts = StringUtils.split(name, '/');
         if(parts[0].equals("authentication_schemes") && parts.length <= 2) {
            if(parts.length == 1) {
               return AuthenticationSchemeResource.ROOT_RESOURCE;
            } else {
               Optional<AuthenticationScheme> authenticationScheme = AuthenticationScheme.optionalValueOf(parts[1]);
               if(!authenticationScheme.isPresent()) {
                  throw new IllegalArgumentException(String.format("%s is not a valid authentication resource name", new Object[]{name}));
               } else {
                  return AuthenticationSchemeResource.scheme((AuthenticationScheme)authenticationScheme.get());
               }
            }
         } else {
            throw new IllegalArgumentException(String.format("%s is not a valid authentication resource name", new Object[]{name}));
         }
      }
   }

   static enum Level {
      ROOT,
      SCHEME;

      private Level() {
      }
   }
}
