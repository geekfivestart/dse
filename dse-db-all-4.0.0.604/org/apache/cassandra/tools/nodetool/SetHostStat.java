package org.apache.cassandra.tools.nodetool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SetHostStat implements Iterable<HostStat> {
   final List<HostStat> hostStats = new ArrayList();
   final boolean resolveIp;

   public SetHostStat(boolean resolveIp) {
      this.resolveIp = resolveIp;
   }

   public int size() {
      return this.hostStats.size();
   }

   public Iterator<HostStat> iterator() {
      return this.hostStats.iterator();
   }

   public void add(String token, String host, Map<InetAddress, Float> ownerships) throws UnknownHostException {
      InetAddress endpoint = InetAddress.getByName(host);
      Float owns = (Float)ownerships.get(endpoint);
      this.hostStats.add(new HostStat(token, endpoint, this.resolveIp, owns));
   }
}
