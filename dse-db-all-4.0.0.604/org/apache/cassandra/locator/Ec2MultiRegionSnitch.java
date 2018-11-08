package org.apache.cassandra.locator;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;

public class Ec2MultiRegionSnitch extends Ec2Snitch {
   private static final String PUBLIC_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/public-ipv4";
   private static final String PRIVATE_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/local-ipv4";
   private final String localPrivateAddress;

   public Ec2MultiRegionSnitch() throws IOException, ConfigurationException {
      InetAddress localPublicAddress = InetAddress.getByName(this.awsApiCall("http://169.254.169.254/latest/meta-data/public-ipv4"));
      logger.info("EC2Snitch using publicIP as identifier: {}", localPublicAddress);
      this.localPrivateAddress = this.awsApiCall("http://169.254.169.254/latest/meta-data/local-ipv4");
      DatabaseDescriptor.setBroadcastAddress(localPublicAddress);
      if(DatabaseDescriptor.getBroadcastNativeTransportAddress() == null) {
         logger.info("broadcast_rpc_address unset, broadcasting public IP as rpc_address: {}", localPublicAddress);
         DatabaseDescriptor.setBroadcastNativeTransportAddress(localPublicAddress);
      }

   }

   public void gossiperStarting() {
      super.gossiperStarting();
      Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP, StorageService.instance.valueFactory.internalIP(this.localPrivateAddress));
      Gossiper.instance.register(new ReconnectableSnitchHelper(this, this.ec2region, true));
   }

   public String toString() {
      return "Ec2MultiRegionSnitch{, localPrivateAddress=" + this.localPrivateAddress + ", region(DC)='" + this.ec2region + '\'' + ", zone(rack)='" + this.ec2zone + "'}";
   }
}
