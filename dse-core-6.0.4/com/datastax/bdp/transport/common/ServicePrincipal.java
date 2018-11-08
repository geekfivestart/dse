package com.datastax.bdp.transport.common;

import com.datastax.bdp.util.Addresses;

public class ServicePrincipal {
   public final String service;
   public final String host;
   public final String realm;

   public ServicePrincipal(String principal) {
      String[] names = principal.split("[/@]");
      if(names.length != 3) {
         throw new ServicePrincipalFormatException("Invalid format kerberos service principal: " + principal + ". Service principal must be of the form: <service name>/<host name>@<realm>");
      } else {
         this.service = names[0];
         this.host = names[1];
         this.realm = names[2];
      }
   }

   public String asLocal() {
      return this.service + "/" + this.getLocalHostName() + "@" + this.realm;
   }

   public String asPattern() {
      return this.service + "/_HOST@" + this.realm;
   }

   public String getLocalHostName() {
      return this.host.equals("_HOST")?Addresses.Internode.getBroadcastAddress().getHostName().toLowerCase():this.host;
   }
}
