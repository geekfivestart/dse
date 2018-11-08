package com.datastax.bdp.config;

public class KerberosOptions {
   public String keytab;
   public String service_principal;
   public String http_principal;
   public String qop;

   public KerberosOptions() {
   }
}
