package org.apache.cassandra.locator;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.config.DatabaseDescriptor;

public class EndpointSnitchInfo implements EndpointSnitchInfoMBean {
   public EndpointSnitchInfo() {
   }

   public static void create() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(new EndpointSnitchInfo(), new ObjectName("org.apache.cassandra.db:type=EndpointSnitchInfo"));
      } catch (Exception var2) {
         throw new RuntimeException(var2);
      }
   }

   public String getDatacenter(String host) throws UnknownHostException {
      return DatabaseDescriptor.getEndpointSnitch().getDatacenter(InetAddress.getByName(host));
   }

   public String getRack(String host) throws UnknownHostException {
      return DatabaseDescriptor.getEndpointSnitch().getRack(InetAddress.getByName(host));
   }

   public String getDatacenter() {
      return DatabaseDescriptor.getLocalDataCenter();
   }

   public String getRack() {
      return DatabaseDescriptor.getLocalRack();
   }

   public String getSnitchName() {
      return DatabaseDescriptor.getEndpointSnitch().getClass().getName();
   }

   public String getDisplayName() {
      return DatabaseDescriptor.getEndpointSnitch().getDisplayName();
   }

   public boolean isDynamicSnitch() {
      return DynamicEndpointSnitch.class.getName().equals(this.getSnitchName());
   }
}
