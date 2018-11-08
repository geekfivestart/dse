package com.datastax.bdp.config;

import java.util.Set;

public class AuthenticationOptions {
   public Boolean enabled;
   public String default_scheme;
   public Set<String> other_schemes;
   public Boolean scheme_permissions;
   public Boolean allow_digest_with_kerberos;
   public String plain_text_without_ssl;
   public String transitional_mode;

   public AuthenticationOptions() {
   }
}
