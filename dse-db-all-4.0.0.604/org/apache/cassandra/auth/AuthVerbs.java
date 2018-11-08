package org.apache.cassandra.auth;

import java.util.function.Function;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class AuthVerbs extends VerbGroup<AuthVerbs.AuthVersion> {
   public final Verb.OneWay<RoleInvalidation> INVALIDATE;

   public AuthVerbs(Verbs.Group id) {
      super(id, true, AuthVerbs.AuthVersion.class);
      VerbGroup<AuthVerbs.AuthVersion>.RegistrationHelper helper = this.helper().stage(Stage.AUTHZ);
      this.INVALIDATE = helper.oneWay("INVALIDATE_ROLE", RoleInvalidation.class).handler((from, invalidation) -> {
         DatabaseDescriptor.getAuthManager().handleRoleInvalidation(invalidation);
      });
   }

   public static enum AuthVersion implements Version<AuthVerbs.AuthVersion> {
      DSE_60(EncodingVersion.OSS_30);

      public final EncodingVersion encodingVersion;

      private AuthVersion(EncodingVersion encodingVersion) {
         this.encodingVersion = encodingVersion;
      }

      public static <T> Versioned<AuthVerbs.AuthVersion, T> versioned(Function<AuthVerbs.AuthVersion, ? extends T> function) {
         return new Versioned(AuthVerbs.AuthVersion.class, function);
      }
   }
}
