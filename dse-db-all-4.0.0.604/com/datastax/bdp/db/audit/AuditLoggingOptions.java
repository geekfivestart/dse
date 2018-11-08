package com.datastax.bdp.db.audit;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.StringUtils;

public class AuditLoggingOptions {
   public boolean enabled = false;
   public String logger = SLF4JAuditWriter.class.getName();
   public String included_categories = null;
   public String excluded_categories = null;
   public String included_keyspaces = null;
   public String excluded_keyspaces = null;
   public String included_roles = null;
   public String excluded_roles = null;
   public int retention_time = 0;
   public CassandraAuditWriterOptions cassandra_audit_writer_options = new CassandraAuditWriterOptions();

   public AuditLoggingOptions() {
   }

   public void validateFilters() {
      if(StringUtils.isNotBlank(this.excluded_keyspaces) && StringUtils.isNotBlank(this.included_keyspaces)) {
         throw new ConfigurationException("Can't specify both included and excluded keyspaces for audit logger", false);
      } else if(StringUtils.isNotBlank(this.excluded_categories) && StringUtils.isNotBlank(this.included_categories)) {
         throw new ConfigurationException("Can't specify both included and excluded categories for audit logger", false);
      } else if(StringUtils.isNotBlank(this.excluded_roles) && StringUtils.isNotBlank(this.included_roles)) {
         throw new ConfigurationException("Can't specify both included and excluded roles for audit logger", false);
      }
   }
}
