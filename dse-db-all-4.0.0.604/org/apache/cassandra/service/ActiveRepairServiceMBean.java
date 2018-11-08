package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;

public interface ActiveRepairServiceMBean {
   String MBEAN_NAME = "org.apache.cassandra.db:type=RepairService";

   List<Map<String, String>> getSessions(boolean var1);

   void failSession(String var1, boolean var2);
}
