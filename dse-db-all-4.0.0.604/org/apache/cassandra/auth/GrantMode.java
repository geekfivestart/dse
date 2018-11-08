package org.apache.cassandra.auth;

public enum GrantMode {
   GRANT {
      public String grantOperationName() {
         return "GRANT";
      }

      public String revokeOperationName() {
         return "REVOKE";
      }

      public String revokeWarningMessage(String roleName, IResource resource, String permissions) {
         return String.format("Role '%s' was not granted %s on %s", new Object[]{roleName, permissions, resource});
      }

      public String grantWarningMessage(String roleName, IResource resource, String permissions) {
         return String.format("Role '%s' was already granted %s on %s", new Object[]{roleName, permissions, resource});
      }
   },
   RESTRICT {
      public String grantOperationName() {
         return "RESTRICT";
      }

      public String revokeOperationName() {
         return "UNRESTRICT";
      }

      public String revokeWarningMessage(String roleName, IResource resource, String permissions) {
         return String.format("Role '%s' was not restricted %s on %s", new Object[]{roleName, permissions, resource});
      }

      public String grantWarningMessage(String roleName, IResource resource, String permissions) {
         return String.format("Role '%s' was already restricted %s on %s", new Object[]{roleName, permissions, resource});
      }
   },
   GRANTABLE {
      public String grantOperationName() {
         return "GRANT AUTHORIZE FOR";
      }

      public String revokeOperationName() {
         return "REVOKE AUTHORIZE FOR";
      }

      public String revokeWarningMessage(String roleName, IResource resource, String permissions) {
         return String.format("Role '%s' was not granted AUTHORIZE FOR %s on %s", new Object[]{roleName, permissions, resource});
      }

      public String grantWarningMessage(String roleName, IResource resource, String permissions) {
         return String.format("Role '%s' was already granted AUTHORIZE FOR %s on %s", new Object[]{roleName, permissions, resource});
      }
   };

   private GrantMode() {
   }

   public abstract String grantOperationName();

   public abstract String revokeOperationName();

   public abstract String revokeWarningMessage(String var1, IResource var2, String var3);

   public abstract String grantWarningMessage(String var1, IResource var2, String var3);
}
