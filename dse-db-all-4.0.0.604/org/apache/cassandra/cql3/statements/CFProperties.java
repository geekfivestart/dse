package org.apache.cassandra.cql3.statements;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;

public class CFProperties {
   public final TableAttributes properties = new TableAttributes();
   final Map<ColumnIdentifier, Boolean> definedOrdering = new LinkedHashMap();
   boolean useCompactStorage = false;

   public CFProperties() {
   }

   public void validate() {
      this.properties.validate();
   }

   public void setOrdering(ColumnIdentifier alias, boolean reversed) {
      this.definedOrdering.put(alias, Boolean.valueOf(reversed));
   }

   public void setCompactStorage() {
      this.useCompactStorage = true;
   }

   public AbstractType getReversableType(ColumnIdentifier targetIdentifier, AbstractType<?> type) {
      return (AbstractType)(!this.definedOrdering.containsKey(targetIdentifier)?type:(((Boolean)this.definedOrdering.get(targetIdentifier)).booleanValue()?ReversedType.getInstance(type):type));
   }
}
