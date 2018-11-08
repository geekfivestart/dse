package com.datastax.bdp.plugin;

import com.datastax.bdp.plugin.bean.SettableTTLMXBean;
import com.google.common.base.Objects;
import java.beans.ConstructorProperties;
import java.beans.PropertyVetoException;
import java.util.Comparator;
import java.util.List;

public interface CqlSlowLogMXBean extends SettableTTLMXBean, ConfigExportableMXBean {
   Comparator<CqlSlowLogMXBean.SlowCqlQuery> SLOW_CQL_QUERY_COMPARATOR = (o1, o2) -> {
      return Long.compare(o2.getDuration(), o1.getDuration());
   };

   double getThreshold();

   void setThreshold(double var1) throws PropertyVetoException;

   long getEffectiveThreshold();

   long getMinimumSamples();

   void setMinimumSamples(long var1) throws PropertyVetoException;

   boolean isSkipWritingToDB();

   void setSkipWritingToDB(boolean var1);

   int getNumSlowestQueries();

   void setNumSlowestQueries(int var1) throws PropertyVetoException;

   List<CqlSlowLogMXBean.SlowCqlQuery> retrieveRecentSlowestCqlQueries();

   public static class SlowCqlQuery implements Comparable<CqlSlowLogMXBean.SlowCqlQuery> {
      public final String tables;
      public final String sourceIp;
      public final String username;
      public final String startTimeUUID;
      public final long duration;
      public final String cqlStrings;
      public final String tracingSessionId;

      @ConstructorProperties({"tables", "sourceIp", "username", "startTimeUUID", "duration", "cqlStrings", "tracingSessionId"})
      public SlowCqlQuery(String tables, String sourceIp, String username, String startTimeUUID, long duration, String cqlStrings, String tracingSessionId) {
         this.tables = tables;
         this.sourceIp = sourceIp;
         this.username = username;
         this.startTimeUUID = startTimeUUID;
         this.duration = duration;
         this.cqlStrings = cqlStrings;
         this.tracingSessionId = tracingSessionId;
      }

      public String getTables() {
         return this.tables;
      }

      public String getSourceIp() {
         return this.sourceIp;
      }

      public String getUsername() {
         return this.username;
      }

      public String getStartTimeUUID() {
         return this.startTimeUUID;
      }

      public long getDuration() {
         return this.duration;
      }

      public String getCqlStrings() {
         return this.cqlStrings;
      }

      public String getTracingSessionId() {
         return this.tracingSessionId;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            CqlSlowLogMXBean.SlowCqlQuery that = (CqlSlowLogMXBean.SlowCqlQuery)o;
            return this.duration == that.duration && Objects.equal(this.tables, that.tables) && Objects.equal(this.sourceIp, that.sourceIp) && Objects.equal(this.username, that.username) && Objects.equal(this.startTimeUUID, that.startTimeUUID) && Objects.equal(this.cqlStrings, that.cqlStrings) && Objects.equal(this.tracingSessionId, that.tracingSessionId);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hashCode(new Object[]{this.tables, this.sourceIp, this.username, this.startTimeUUID, Long.valueOf(this.duration), this.cqlStrings, this.tracingSessionId});
      }

      public int compareTo(CqlSlowLogMXBean.SlowCqlQuery other) {
         return Long.compare(this.duration, other.duration);
      }

      public String toString() {
         StringBuilder sb = new StringBuilder("SlowCqlQuery{");
         sb.append("tables=").append(this.tables);
         sb.append(", sourceIp='").append(this.sourceIp).append('\'');
         sb.append(", username='").append(this.username).append('\'');
         sb.append(", startTimeUUID='").append(this.startTimeUUID).append('\'');
         sb.append(", duration=").append(this.duration);
         sb.append(", cqlStrings=").append(this.cqlStrings);
         sb.append(", tracingSessionId=").append(this.tracingSessionId);
         sb.append('}');
         return sb.toString();
      }
   }
}
