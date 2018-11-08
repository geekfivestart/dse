package com.datastax.bdp.config;

import java.util.Objects;

public class AlwaysOnSqlOptions {
   public Boolean enabled;
   public String thrift_port;
   public String web_ui_port;
   public String reserve_port_wait_time_ms;
   public String alwayson_sql_status_check_wait_time_ms;
   public String log_dsefs_dir;
   public String workpool;
   public String auth_user;
   public String runner_max_errors;

   public AlwaysOnSqlOptions() {
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         AlwaysOnSqlOptions that = (AlwaysOnSqlOptions)o;
         return Objects.equals(this.enabled, that.enabled) && Objects.equals(this.thrift_port, that.thrift_port) && Objects.equals(this.web_ui_port, that.web_ui_port) && Objects.equals(this.reserve_port_wait_time_ms, that.reserve_port_wait_time_ms) && Objects.equals(this.alwayson_sql_status_check_wait_time_ms, that.alwayson_sql_status_check_wait_time_ms) && Objects.equals(this.log_dsefs_dir, that.log_dsefs_dir) && Objects.equals(this.workpool, that.workpool) && Objects.equals(this.auth_user, that.auth_user) && Objects.equals(this.runner_max_errors, that.runner_max_errors);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.enabled, this.thrift_port, this.web_ui_port, this.reserve_port_wait_time_ms, this.alwayson_sql_status_check_wait_time_ms, this.log_dsefs_dir, this.workpool, this.auth_user, this.runner_max_errors});
   }
}
