package com.datastax.bdp.config;

public class LdapOptions {
   public String server_host;
   public String server_port;
   public boolean use_ssl;
   public boolean use_tls;
   public String truststore_path;
   public String truststore_password;
   public String truststore_type;
   public String search_dn;
   public String search_password;
   public String user_search_base;
   public String user_search_filter;
   public String user_memberof_attribute;
   public String group_search_type;
   public String group_search_base;
   public String group_search_filter;
   public String group_name_attribute;
   public String credentials_validity_in_ms;
   public String search_validity_in_seconds;
   public LdapConnectionPoolOptions connection_pool = new LdapConnectionPoolOptions();

   public LdapOptions() {
   }
}
