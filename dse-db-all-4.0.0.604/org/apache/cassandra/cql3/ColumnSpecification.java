package org.apache.cassandra.cql3;

import com.google.common.base.MoreObjects;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;

public class ColumnSpecification {
   public final String ksName;
   public final String cfName;
   public final ColumnIdentifier name;
   public final AbstractType<?> type;

   public ColumnSpecification(String ksName, String cfName, ColumnIdentifier name, AbstractType<?> type) {
      this.ksName = ksName;
      this.cfName = cfName;
      this.name = name;
      this.type = type;
   }

   public ColumnSpecification withAlias(ColumnIdentifier alias) {
      return new ColumnSpecification(this.ksName, this.cfName, alias, this.type);
   }

   public boolean isReversedType() {
      return this.type instanceof ReversedType;
   }

   public static boolean allInSameTable(Collection<ColumnSpecification> names) {
      if(names != null && !names.isEmpty()) {
         Iterator<ColumnSpecification> iter = names.iterator();
         ColumnSpecification first = (ColumnSpecification)iter.next();

         ColumnSpecification name;
         do {
            if(!iter.hasNext()) {
               return true;
            }

            name = (ColumnSpecification)iter.next();
         } while(name.ksName.equals(first.ksName) && name.cfName.equals(first.cfName));

         return false;
      } else {
         return false;
      }
   }

   public boolean equals(Object other) {
      if(!(other instanceof ColumnSpecification)) {
         return false;
      } else {
         ColumnSpecification that = (ColumnSpecification)other;
         return this.ksName.equals(that.ksName) && this.cfName.equals(that.cfName) && this.name.equals(that.name) && this.type.equals(that.type);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.ksName, this.cfName, this.name, this.type});
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("name", this.name).add("type", this.type).toString();
   }
}
