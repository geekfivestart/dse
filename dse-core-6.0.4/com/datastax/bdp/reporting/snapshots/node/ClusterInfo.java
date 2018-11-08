package com.datastax.bdp.reporting.snapshots.node;

import com.datastax.bdp.reporting.CqlWritable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

public class ClusterInfo implements CqlWritable, Iterable<DatacenterInfo> {
   private Map<String, DatacenterInfo> datacenters = new HashMap();

   public ClusterInfo() {
   }

   public void addNode(NodeInfo node) {
      DatacenterInfo dc = (DatacenterInfo)this.datacenters.get(node.datacenter);
      if(dc == null) {
         dc = new DatacenterInfo(node.datacenter);
         this.datacenters.put(dc.name, dc);
      }

      dc.addNode(node);
   }

   public Iterator<DatacenterInfo> iterator() {
      return this.datacenters.values().iterator();
   }

   public List<ByteBuffer> toByteBufferList(CassandraVersion cassandraVersion) {
      Set<String> dcNames = new HashSet();
      AggregateNodeInfo summary = new AggregateNodeInfo();
      Iterator var4 = this.datacenters.values().iterator();

      while(var4.hasNext()) {
         DatacenterInfo dc = (DatacenterInfo)var4.next();
         dcNames.add(dc.name);
         summary.add(dc.getSummaryStats());
      }

      List<ByteBuffer> vars = summary.toByteBufferList(cassandraVersion);
      vars.add(0, ByteBufferUtil.bytes(Schema.instance.getNumberOfTables()));
      vars.add(0, ByteBufferUtil.bytes(Schema.instance.getKeyspaces().size()));
      vars.add(0, SetType.getInstance(UTF8Type.instance, true).decompose(dcNames));
      return vars;
   }
}
