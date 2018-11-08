package org.apache.cassandra.cql3.selection;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.TableMetadata;

public class RawSelector {
   public final Selectable.Raw selectable;
   public final ColumnIdentifier alias;

   public RawSelector(Selectable.Raw selectable, ColumnIdentifier alias) {
      this.selectable = selectable;
      this.alias = alias;
   }

   public static List<Selectable> toSelectables(List<RawSelector> raws, TableMetadata table) {
      return Lists.transform(raws, (raw) -> {
         return raw.prepare(table);
      });
   }

   private Selectable prepare(TableMetadata table) {
      Selectable s = this.selectable.prepare(table);
      return (Selectable)(this.alias != null?new AliasedSelectable(s, this.alias):s);
   }
}
