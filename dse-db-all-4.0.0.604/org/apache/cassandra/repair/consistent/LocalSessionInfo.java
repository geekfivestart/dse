package org.apache.cassandra.repair.consistent;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public class LocalSessionInfo {
   public static final String SESSION_ID = "SESSION_ID";
   public static final String STATE = "STATE";
   public static final String STARTED = "STARTED";
   public static final String LAST_UPDATE = "LAST_UPDATE";
   public static final String COORDINATOR = "COORDINATOR";
   public static final String PARTICIPANTS = "PARTICIPANTS";
   public static final String TABLES = "TABLES";

   private LocalSessionInfo() {
   }

   private static String tableString(TableId id) {
      TableMetadata meta = Schema.instance.getTableMetadata(id);
      return meta != null?meta.keyspace + '.' + meta.name:"<null>";
   }

   static Map<String, String> sessionToMap(LocalSession session) {
      Map<String, String> m = new HashMap();
      m.put("SESSION_ID", session.sessionID.toString());
      m.put("STATE", session.getState().toString());
      m.put("STARTED", Integer.toString(session.getStartedAt()));
      m.put("LAST_UPDATE", Integer.toString(session.getLastUpdate()));
      m.put("COORDINATOR", session.coordinator.toString());
      m.put("PARTICIPANTS", Joiner.on(',').join(Iterables.transform(session.participants, InetAddress::toString)));
      m.put("TABLES", Joiner.on(',').join(Iterables.transform(session.tableIds, LocalSessionInfo::tableString)));
      return m;
   }
}
