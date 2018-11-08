package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public final class TriggerMetadata {
   public static final String CLASS = "class";
   public final String name;
   public final String classOption;

   public TriggerMetadata(String name, String classOption) {
      this.name = name;
      this.classOption = classOption;
   }

   public static TriggerMetadata create(String name, String classOption) {
      return new TriggerMetadata(name, classOption);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof TriggerMetadata)) {
         return false;
      } else {
         TriggerMetadata td = (TriggerMetadata)o;
         return this.name.equals(td.name) && this.classOption.equals(td.classOption);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.name, this.classOption});
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("name", this.name).add("class", this.classOption).toString();
   }
}
