package com.datastax.bdp.snitch;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.gms.DseState;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.util.Addresses;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseDelegateSnitch extends AbstractNetworkTopologySnitch {
   protected static Logger logger = LoggerFactory.getLogger(DseDelegateSnitch.class);
   protected Set<Workload> myWorkloads = SystemInfo.getWorkloads();
   protected IEndpointSnitch delegatedSnitch;

   public DseDelegateSnitch() throws ConfigurationException {
      logger.info("Setting my workloads to " + this.myWorkloads.toString());
      this.delegatedSnitch = DseConfig.getDelegatedSnitch();
      logger.info("Initialized DseDelegateSnitch with workloads {}, delegating to {}", this.myWorkloads.toString(), this.delegatedSnitch.getClass().getName());
   }

   public IEndpointSnitch getDelegatedSnitch() {
      return this.delegatedSnitch;
   }

   public String getDisplayName() {
      return this.getDelegatedSnitch().getDisplayName();
   }

   public String toString() {
      return "DseDelegateSnitch{workloads=" + this.myWorkloads + ", delegate=" + this.delegatedSnitch + '}';
   }

   public String getDatacenter(InetAddress endpoint) {
      return this.delegatedSnitch.getDatacenter(endpoint);
   }

   public void gossiperStarting() {
      DseState.instance.initialize();
      this.delegatedSnitch.gossiperStarting();
   }

   public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> endpoints) {
      return this.delegatedSnitch.getSortedListByProximity(address, endpoints);
   }

   public void sortByProximity(InetAddress address, List<InetAddress> endpoints) {
      this.delegatedSnitch.sortByProximity(address, endpoints);
   }

   public boolean isDefaultDC(String dc) {
      return this.delegatedSnitch.isDefaultDC(dc);
   }

   public String getRack(InetAddress address) {
      return this.delegatedSnitch.getRack(address);
   }

   public int compareEndpoints(InetAddress address, InetAddress a1, InetAddress a2) {
      Set<Workload> workloads = this.getEndpointWorkloads(address);
      Set<Workload> workloads1 = this.getEndpointWorkloads(a1);
      Set<Workload> workloads2 = this.getEndpointWorkloads(a2);
      Iterator var7 = workloads.iterator();

      Workload w;
      do {
         if(!var7.hasNext()) {
            return this.delegatedSnitch.compareEndpoints(address, a1, a2);
         }

         w = (Workload)var7.next();
         if(w.isCompatibleWith(workloads1) && !w.isCompatibleWith(workloads2)) {
            return -1;
         }
      } while(!w.isCompatibleWith(workloads2) || w.isCompatibleWith(workloads1));

      return 1;
   }

   public String getDatacenterWithWorkloads(InetAddress endpoint) {
      return this.getDatacenter(endpoint) + "-" + Workload.workloadNames(this.getEndpointWorkloads(endpoint));
   }

   public Set<Workload> getEndpointWorkloads(InetAddress endpoint) {
      Set<Workload> workloads = EndpointStateTracker.instance.getWorkloadsIfPresent(endpoint);
      if(workloads.contains(Workload.Unknown) && Addresses.Internode.isLocalEndpoint(endpoint)) {
         workloads = this.myWorkloads;
      }

      if(logger.isDebugEnabled()) {
         logger.debug("Workloads for endpoint " + endpoint.getHostAddress() + " is " + this.myWorkloads.toString());
      }

      return workloads;
   }
}
