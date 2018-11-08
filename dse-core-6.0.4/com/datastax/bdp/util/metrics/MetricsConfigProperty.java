package com.datastax.bdp.util.metrics;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class MetricsConfigProperty {
   private String instance;
   private String kind;
   private String name;
   private String option;

   public MetricsConfigProperty(String instance, String kind, String name, String option) {
      this.instance = instance;
      this.kind = kind;
      this.name = name;
      this.option = option;
   }

   public String getInstance() {
      return this.instance;
   }

   public void setInstance(String instance) {
      this.instance = instance;
   }

   public String getKind() {
      return this.kind;
   }

   public void setKind(String kind) {
      this.kind = kind;
   }

   public String getName() {
      return this.name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getOption() {
      return this.option;
   }

   public void setOption(String option) {
      this.option = option;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         MetricsConfigProperty that = (MetricsConfigProperty)o;
         return (new EqualsBuilder()).append(this.instance, that.instance).append(this.kind, that.kind).append(this.name, that.name).append(this.option, that.option).isEquals();
      } else {
         return false;
      }
   }

   public int hashCode() {
      return (new HashCodeBuilder(17, 37)).append(this.instance).append(this.kind).append(this.name).append(this.option).toHashCode();
   }

   public String getPropertyName() {
      return String.format("%s.%s.%s.%s", new Object[]{this.instance, this.kind, this.name, this.option});
   }
}
