package org.apache.cassandra.auth;

import com.google.common.collect.ComparisonChain;
import java.util.Objects;
import java.util.Set;

public class PermissionDetails implements Comparable<PermissionDetails> {
   public final String grantee;
   public final IResource resource;
   public final Permission permission;
   public final Set<GrantMode> modes;

   public PermissionDetails(String grantee, IResource resource, Permission permission, Set<GrantMode> modes) {
      this.grantee = grantee;
      this.resource = resource;
      this.permission = permission;
      this.modes = modes;
   }

   public int compareTo(PermissionDetails other) {
      return ComparisonChain.start().compare(this.grantee, other.grantee).compare(this.resource.getName(), other.resource.getName()).compare(this.permission.domain(), other.permission.domain()).compare(this.permission.ordinal(), other.permission.ordinal()).result();
   }

   public String toString() {
      return String.format("<PermissionDetails grantee:%s resource:%s permission:%s>", new Object[]{this.grantee, this.resource.getName(), this.permission});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof PermissionDetails)) {
         return false;
      } else {
         PermissionDetails pd = (PermissionDetails)o;
         return Objects.equals(this.grantee, pd.grantee) && Objects.equals(this.resource, pd.resource) && Objects.equals(this.permission, pd.permission);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.grantee, this.resource, this.permission});
   }
}
