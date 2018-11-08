package org.apache.cassandra.db;

import com.google.common.base.Throwables;
import java.util.Map.Entry;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

public class SnapshotDetailsTabularData {
   private static final String[] ITEM_NAMES = new String[]{"Snapshot name", "Keyspace name", "Column family name", "True size", "Size on disk"};
   private static final String[] ITEM_DESCS = new String[]{"snapshot_name", "keyspace_name", "columnfamily_name", "TrueDiskSpaceUsed", "TotalDiskSpaceUsed"};
   private static final String TYPE_NAME = "SnapshotDetails";
   private static final String ROW_DESC = "SnapshotDetails";
   private static final OpenType<?>[] ITEM_TYPES;
   private static final CompositeType COMPOSITE_TYPE;
   public static final TabularType TABULAR_TYPE;

   public SnapshotDetailsTabularData() {
   }

   public static void from(String snapshot, String ks, String cf, Entry<String, Pair<Long, Long>> snapshotDetail, TabularDataSupport result) {
      try {
         String totalSize = FileUtils.stringifyFileSize((double)((Long)((Pair)snapshotDetail.getValue()).left).longValue());
         String liveSize = FileUtils.stringifyFileSize((double)((Long)((Pair)snapshotDetail.getValue()).right).longValue());
         result.put(new CompositeDataSupport(COMPOSITE_TYPE, ITEM_NAMES, new Object[]{snapshot, ks, cf, liveSize, totalSize}));
      } catch (OpenDataException var7) {
         throw new RuntimeException(var7);
      }
   }

   static {
      try {
         ITEM_TYPES = new OpenType[]{SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING};
         COMPOSITE_TYPE = new CompositeType("SnapshotDetails", "SnapshotDetails", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
         TABULAR_TYPE = new TabularType("SnapshotDetails", "SnapshotDetails", COMPOSITE_TYPE, ITEM_NAMES);
      } catch (OpenDataException var1) {
         throw Throwables.propagate(var1);
      }
   }
}
