package com.datastax.bdp.reporting.snapshots.db;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DatabaseInfo implements Iterable<KeyspaceInfo> {
   public final String nodeAddress;
   private final Map<String, KeyspaceInfo> keyspaces = new HashMap();

   public DatabaseInfo() {
      this.nodeAddress = null;
   }

   public DatabaseInfo(String nodeAddress) {
      this.nodeAddress = nodeAddress;
   }

   public void addTableInfo(TableInfo tableInfo) {
      KeyspaceInfo ks = (KeyspaceInfo)this.keyspaces.get(tableInfo.ksName);
      if(ks == null) {
         ks = new KeyspaceInfo(tableInfo.ksName);
         this.keyspaces.put(ks.name, ks);
      }

      ks.addTableInfo(tableInfo);
   }

   public KeyspaceInfo getKeyspaceInfo(String name) {
      return (KeyspaceInfo)this.keyspaces.get(name);
   }

   public void addKeyspaceInfo(KeyspaceInfo info) {
      this.keyspaces.put(info.name, info);
   }

   public Set<String> getKeyspaceNames() {
      return this.keyspaces.keySet();
   }

   public void aggregate(Iterable<DatabaseInfo> infos) {
      Iterator var2 = infos.iterator();

      while(var2.hasNext()) {
         DatabaseInfo dbInfo = (DatabaseInfo)var2.next();
         Set<String> intersection = Sets.intersection(this.keyspaces.keySet(), dbInfo.getKeyspaceNames());
         Iterator var5 = intersection.iterator();

         while(var5.hasNext()) {
            String ksName = (String)var5.next();
            ((KeyspaceInfo)this.keyspaces.get(ksName)).aggregate(dbInfo.getKeyspaceInfo(ksName));
         }

         Set<String> onlyInOther = Sets.difference(dbInfo.getKeyspaceNames(), this.keyspaces.keySet());
         Iterator var9 = onlyInOther.iterator();

         while(var9.hasNext()) {
            String ksName = (String)var9.next();
            this.addKeyspaceInfo(dbInfo.getKeyspaceInfo(ksName));
         }
      }

   }

   public Iterator<KeyspaceInfo> iterator() {
      return this.keyspaces.values().iterator();
   }
}
