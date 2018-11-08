package org.apache.cassandra.auth;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.apache.commons.lang3.StringUtils;

public class RoleResource implements IResource, Comparable<RoleResource> {
   public static final Versioned<EncodingVersion, RoleResource.RoleResourceSerializer> rawSerializers = EncodingVersion.versioned(RoleResource.RoleResourceSerializer::<init>);
   public static final Versioned<AuthVerbs.AuthVersion, Serializer<RoleResource>> serializers = AuthVerbs.AuthVersion.versioned((v) -> {
      return (RoleResource.RoleResourceSerializer)rawSerializers.get(v.encodingVersion);
   });
   private static final Set<Permission> ROOT_LEVEL_PERMISSIONS;
   private static final Set<Permission> ROLE_LEVEL_PERMISSIONS;
   private static final String ROOT_NAME = "roles";
   private static final RoleResource ROOT_RESOURCE;
   private final RoleResource.Level level;
   private final String name;

   private RoleResource() {
      this.level = RoleResource.Level.ROOT;
      this.name = null;
   }

   private RoleResource(String name) {
      this.level = RoleResource.Level.ROLE;
      this.name = name;
   }

   public static RoleResource root() {
      return ROOT_RESOURCE;
   }

   public static RoleResource role(String name) {
      return new RoleResource(name);
   }

   public static RoleResource fromName(String name) {
      String[] parts = StringUtils.split(name, "/", 2);
      if(!parts[0].equals("roles")) {
         throw new IllegalArgumentException(String.format("%s is not a valid role resource name", new Object[]{name}));
      } else {
         return parts.length == 1?root():role(parts[1]);
      }
   }

   public String getName() {
      return this.level == RoleResource.Level.ROOT?"roles":String.format("%s/%s", new Object[]{"roles", this.name});
   }

   public String getRoleName() {
      if(this.level == RoleResource.Level.ROOT) {
         throw new IllegalStateException(String.format("%s role resource has no role name", new Object[]{this.level}));
      } else {
         return this.name;
      }
   }

   public IResource getParent() {
      if(this.level == RoleResource.Level.ROLE) {
         return root();
      } else {
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean hasParent() {
      return this.level != RoleResource.Level.ROOT;
   }

   public boolean exists() {
      return this.level == RoleResource.Level.ROOT || DatabaseDescriptor.getRoleManager().isExistingRole(this);
   }

   public Set<Permission> applicablePermissions() {
      return this.level == RoleResource.Level.ROOT?ROOT_LEVEL_PERMISSIONS:ROLE_LEVEL_PERMISSIONS;
   }

   public int compareTo(RoleResource o) {
      return this.name.compareTo(o.name);
   }

   public String toString() {
      return this.level == RoleResource.Level.ROOT?"<all roles>":String.format("<role %s>", new Object[]{this.name});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof RoleResource)) {
         return false;
      } else {
         RoleResource rs = (RoleResource)o;
         return Objects.equals(this.level, rs.level) && Objects.equals(this.name, rs.name);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.level, this.name});
   }

   static {
      ROOT_LEVEL_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.CREATE, CorePermission.ALTER, CorePermission.DROP, CorePermission.AUTHORIZE, CorePermission.DESCRIBE});
      ROLE_LEVEL_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.ALTER, CorePermission.DROP, CorePermission.AUTHORIZE});
      ROOT_RESOURCE = new RoleResource();
   }

   public static final class RoleResourceSerializer extends VersionDependent<EncodingVersion> implements Serializer<RoleResource> {
      public RoleResourceSerializer(EncodingVersion encodingVersion) {
         super(encodingVersion);
      }

      public void serialize(RoleResource roleResource, DataOutputPlus out) throws IOException {
         out.writeUTF(roleResource.getName());
      }

      public RoleResource deserialize(DataInputPlus in) throws IOException {
         String roleName = in.readUTF();
         return RoleResource.fromName(roleName);
      }

      public long serializedSize(RoleResource roleResource) {
         return (long)TypeSizes.sizeof(roleResource.getName());
      }
   }

   static enum Level {
      ROOT,
      ROLE;

      private Level() {
      }
   }
}
