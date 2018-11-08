package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.IEndpointSnitch;

public class TokenRange {
   private final Token.TokenFactory tokenFactory;
   public final Range<Token> range;
   public final List<TokenRange.EndpointDetails> endpoints;

   private TokenRange(Token.TokenFactory tokenFactory, Range<Token> range, List<TokenRange.EndpointDetails> endpoints) {
      this.tokenFactory = tokenFactory;
      this.range = range;
      this.endpoints = endpoints;
   }

   private String toStr(Token tk) {
      return this.tokenFactory.toString(tk);
   }

   public static TokenRange create(Token.TokenFactory tokenFactory, Range<Token> range, List<InetAddress> endpoints) {
      List<TokenRange.EndpointDetails> details = new ArrayList(endpoints.size());
      IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
      Iterator var5 = endpoints.iterator();

      while(var5.hasNext()) {
         InetAddress ep = (InetAddress)var5.next();
         details.add(new TokenRange.EndpointDetails(ep, StorageService.instance.getRpcaddress(ep), snitch.getDatacenter(ep), snitch.getRack(ep)));
      }

      return new TokenRange(tokenFactory, range, details);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("TokenRange(");
      sb.append("start_token:").append(this.toStr((Token)this.range.left));
      sb.append(", end_token:").append(this.toStr((Token)this.range.right));
      List<String> hosts = new ArrayList(this.endpoints.size());
      List<String> rpcs = new ArrayList(this.endpoints.size());
      Iterator var4 = this.endpoints.iterator();

      while(var4.hasNext()) {
         TokenRange.EndpointDetails ep = (TokenRange.EndpointDetails)var4.next();
         hosts.add(ep.host.getHostAddress());
         rpcs.add(ep.rpcAddress);
      }

      sb.append("endpoints:").append(hosts);
      sb.append("rpc_endpoints:").append(rpcs);
      sb.append("endpoint_details:").append(this.endpoints);
      sb.append(")");
      return sb.toString();
   }

   public static class EndpointDetails {
      public final InetAddress host;
      public final String rpcAddress;
      public final String datacenter;
      public final String rack;

      private EndpointDetails(InetAddress host, String rpcAddress, String datacenter, String rack) {
         assert host != null;

         this.host = host;
         this.rpcAddress = rpcAddress;
         this.datacenter = datacenter;
         this.rack = rack;
      }

      public String toString() {
         String dcStr = this.datacenter == null?"":String.format(", datacenter:%s", new Object[]{this.datacenter});
         String rackStr = this.rack == null?"":String.format(", rack:%s", new Object[]{this.rack});
         return String.format("EndpointDetails(host:%s%s%s)", new Object[]{this.host.getHostAddress(), dcStr, rackStr});
      }
   }
}
