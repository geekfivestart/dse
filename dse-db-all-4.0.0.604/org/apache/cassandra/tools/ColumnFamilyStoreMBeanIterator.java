package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;

class ColumnFamilyStoreMBeanIterator implements Iterator<Entry<String, ColumnFamilyStoreMBean>> {
   private MBeanServerConnection mbeanServerConn;
   Iterator<Entry<String, ColumnFamilyStoreMBean>> mbeans;

   public ColumnFamilyStoreMBeanIterator(MBeanServerConnection mbeanServerConn) throws MalformedObjectNameException, NullPointerException, IOException {
      this.mbeanServerConn = mbeanServerConn;
      List<Entry<String, ColumnFamilyStoreMBean>> cfMbeans = this.getCFSMBeans(mbeanServerConn, "ColumnFamilies");
      cfMbeans.addAll(this.getCFSMBeans(mbeanServerConn, "IndexColumnFamilies"));
      Collections.sort(cfMbeans, new Comparator<Entry<String, ColumnFamilyStoreMBean>>() {
         public int compare(Entry<String, ColumnFamilyStoreMBean> e1, Entry<String, ColumnFamilyStoreMBean> e2) {
            int keyspaceNameCmp = ((String)e1.getKey()).compareTo((String)e2.getKey());
            if(keyspaceNameCmp != 0) {
               return keyspaceNameCmp;
            } else {
               String[] e1CF = ((ColumnFamilyStoreMBean)e1.getValue()).getTableName().split("\\.");
               String[] e2CF = ((ColumnFamilyStoreMBean)e2.getValue()).getTableName().split("\\.");

               assert e1CF.length <= 2 && e2CF.length <= 2 : "unexpected split count for table name";

               if(e1CF.length == 1 && e2CF.length == 1) {
                  return e1CF[0].compareTo(e2CF[0]);
               } else {
                  int cfNameCmp = e1CF[0].compareTo(e2CF[0]);
                  return cfNameCmp != 0?cfNameCmp:(e1CF.length == 2 && e2CF.length == 2?e1CF[1].compareTo(e2CF[1]):(e1CF.length == 1?1:-1));
               }
            }
         }
      });
      this.mbeans = cfMbeans.iterator();
   }

   private List<Entry<String, ColumnFamilyStoreMBean>> getCFSMBeans(MBeanServerConnection mbeanServerConn, String type) throws MalformedObjectNameException, IOException {
      ObjectName query = new ObjectName("org.apache.cassandra.db:type=" + type + ",*");
      Set<ObjectName> cfObjects = mbeanServerConn.queryNames(query, (QueryExp)null);
      List<Entry<String, ColumnFamilyStoreMBean>> mbeans = new ArrayList(cfObjects.size());
      Iterator var6 = cfObjects.iterator();

      while(var6.hasNext()) {
         ObjectName n = (ObjectName)var6.next();
         String keyspaceName = n.getKeyProperty("keyspace");
         ColumnFamilyStoreMBean cfsProxy = (ColumnFamilyStoreMBean)JMX.newMBeanProxy(mbeanServerConn, n, ColumnFamilyStoreMBean.class);
         mbeans.add(new SimpleImmutableEntry(keyspaceName, cfsProxy));
      }

      return mbeans;
   }

   public boolean hasNext() {
      return this.mbeans.hasNext();
   }

   public Entry<String, ColumnFamilyStoreMBean> next() {
      return (Entry)this.mbeans.next();
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }
}
