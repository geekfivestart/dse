package org.apache.cassandra.service;

import java.util.HashMap;
import java.util.Map;

public class TableInfo {
   public static final String HAS_VIEWS = "hasViews";
   public static final String IS_VIEW = "isView";
   public static final String WAS_INCREMENTALLY_REPAIRED = "wasIncrementallyRepaired";
   public static final String IS_CDC_ENABLED = "isCdcEnabled";
   public final boolean hasViews;
   public final boolean isView;
   public final boolean wasIncrementallyRepaired;
   public final boolean isCdcEnabled;

   public TableInfo(boolean hasViews, boolean isView, boolean wasIncrementallyRepaired, boolean isCdcEnabled) {
      this.hasViews = hasViews;
      this.isView = isView;
      this.wasIncrementallyRepaired = wasIncrementallyRepaired;
      this.isCdcEnabled = isCdcEnabled;
   }

   public Map<String, String> asMap() {
      Map<String, String> tableInfo = new HashMap();
      tableInfo.put("hasViews", (new Boolean(this.hasViews)).toString());
      tableInfo.put("isView", (new Boolean(this.isView)).toString());
      tableInfo.put("wasIncrementallyRepaired", (new Boolean(this.wasIncrementallyRepaired)).toString());
      tableInfo.put("isCdcEnabled", (new Boolean(this.isCdcEnabled)).toString());
      return tableInfo;
   }

   public static TableInfo fromMap(Map<String, String> tableInfo) {
      boolean hasViews = Boolean.parseBoolean((String)tableInfo.get("hasViews"));
      boolean isView = Boolean.parseBoolean((String)tableInfo.get("isView"));
      boolean hasIncrementallyRepaired = Boolean.parseBoolean((String)tableInfo.get("wasIncrementallyRepaired"));
      boolean isCdcEnabled = Boolean.parseBoolean((String)tableInfo.get("isCdcEnabled"));
      return new TableInfo(hasViews, isView, hasIncrementallyRepaired, isCdcEnabled);
   }

   public boolean isOrHasView() {
      return this.isView || this.hasViews;
   }

   public String toString() {
      return "TableInfo{hasViews=" + this.hasViews + ", isView=" + this.isView + ", wasIncrementallyRepaired=" + this.wasIncrementallyRepaired + ", isCdcEnabled=" + this.isCdcEnabled + '}';
   }
}
