package com.datastax.bdp.cassandra.auth;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;

public enum AuthenticationScheme {
   KERBEROS(SaslMechanism.GSSAPI),
   TOKEN(SaslMechanism.DIGEST),
   LDAP(SaslMechanism.PLAIN),
   INTERNAL(SaslMechanism.PLAIN),
   INPROCESS(SaslMechanism.INPROCESS),
   INCLUSTER(SaslMechanism.INCLUSTER);

   public final SaslMechanism saslMechanism;

   private AuthenticationScheme(SaslMechanism saslMechanism) {
      this.saslMechanism = saslMechanism;
   }

   public static Optional<AuthenticationScheme> optionalValueOf(String value) {
      return Arrays.stream(values()).filter((v) -> {
         return v.name().equalsIgnoreCase(value);
      }).findAny();
   }
}
