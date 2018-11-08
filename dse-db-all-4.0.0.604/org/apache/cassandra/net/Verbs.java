package org.apache.cassandra.net;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.datastax.bdp.db.nodesync.NodeSyncVerbs;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.auth.AuthVerbs;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.WriteVerbs;
import org.apache.cassandra.gms.GossipVerbs;
import org.apache.cassandra.hints.HintsVerbs;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.schema.SchemaVerbs;
import org.apache.cassandra.service.OperationsVerbs;
import org.apache.cassandra.service.paxos.LWTVerbs;

public abstract class Verbs {
   private static final IntObjectHashMap<VerbGroup> groupsByCode = new IntObjectHashMap();
   private static final List<VerbGroup<?>> allGroups = new ArrayList();
   public static final ReadVerbs READS;
   public static final WriteVerbs WRITES;
   public static final LWTVerbs LWT;
   public static final HintsVerbs HINTS;
   public static final OperationsVerbs OPERATIONS;
   public static final GossipVerbs GOSSIP;
   public static final RepairVerbs REPAIR;
   public static final SchemaVerbs SCHEMA;
   public static final AuthVerbs AUTH;
   public static final NodeSyncVerbs NODESYNC;

   private Verbs() {
   }

   private static <G extends VerbGroup> G register(G group) {
      allGroups.add(group);
      VerbGroup previous = (VerbGroup)groupsByCode.put(group.id().serializationCode(), group);

      assert previous == null : "Duplicate code for group " + group + ", already assigned to " + previous;

      return group;
   }

   public static Iterable<VerbGroup<?>> allGroups() {
      return Collections.unmodifiableList(allGroups);
   }

   static VerbGroup fromSerializationCode(int code) {
      VerbGroup group = (VerbGroup)groupsByCode.get(code);
      if(group == null) {
         throw new IllegalArgumentException(String.format("Invalid message group code %d", new Object[]{Integer.valueOf(code)}));
      } else {
         return group;
      }
   }

   static {
      READS = (ReadVerbs)register(new ReadVerbs(Verbs.Group.READS));
      WRITES = (WriteVerbs)register(new WriteVerbs(Verbs.Group.WRITES));
      LWT = (LWTVerbs)register(new LWTVerbs(Verbs.Group.LWT));
      HINTS = (HintsVerbs)register(new HintsVerbs(Verbs.Group.HINTS));
      OPERATIONS = (OperationsVerbs)register(new OperationsVerbs(Verbs.Group.OPERATIONS));
      GOSSIP = (GossipVerbs)register(new GossipVerbs(Verbs.Group.GOSSIP));
      REPAIR = (RepairVerbs)register(new RepairVerbs(Verbs.Group.REPAIR));
      SCHEMA = (SchemaVerbs)register(new SchemaVerbs(Verbs.Group.SCHEMA));
      AUTH = (AuthVerbs)register(new AuthVerbs(Verbs.Group.AUTH));
      NODESYNC = (NodeSyncVerbs)register(new NodeSyncVerbs(Verbs.Group.NODESYNC));
   }

   public static enum Group {
      READS(0),
      WRITES(1),
      LWT(2),
      HINTS(3),
      OPERATIONS(4),
      GOSSIP(5),
      REPAIR(6),
      SCHEMA(7),
      AUTH(8),
      NODESYNC(9);

      private final int code;

      private Group(int code) {
         this.code = code;
      }

      public int serializationCode() {
         return this.code;
      }
   }
}
