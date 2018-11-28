package org.apache.cassandra.cql3.selection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.schema.ColumnMetadata;

public class SelectionColumnMapping implements SelectionColumns {
   private final List<ColumnSpecification> columnSpecifications = new ArrayList();
   private final Multimap<ColumnSpecification, ColumnMetadata> columnMappings = HashMultimap.create();

   private SelectionColumnMapping() {
   }

   protected static SelectionColumnMapping newMapping() {
      return new SelectionColumnMapping();
   }

   protected static SelectionColumnMapping simpleMapping(Iterable<ColumnMetadata> columnDefinitions) {
      SelectionColumnMapping mapping = new SelectionColumnMapping();
      Iterator var2 = columnDefinitions.iterator();

      while(var2.hasNext()) {
         ColumnMetadata def = (ColumnMetadata)var2.next();
         mapping.addMapping(def, (ColumnMetadata)def);
      }

      return mapping;
   }

   protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, ColumnMetadata column) {
      this.columnSpecifications.add(colSpec);
      if(column != null) {
         this.columnMappings.put(colSpec, column);
      }

      return this;
   }

   protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, Iterable<ColumnMetadata> columns) {
      this.columnSpecifications.add(colSpec);
      this.columnMappings.putAll(colSpec, columns);
      return this;
   }

   public List<ColumnSpecification> getColumnSpecifications() {
      return Lists.newArrayList(this.columnSpecifications);
   }

   public Multimap<ColumnSpecification, ColumnMetadata> getMappings() {
      return Multimaps.unmodifiableMultimap(this.columnMappings);
   }

   public boolean equals(Object obj) {
      if(obj == null) {
         return false;
      } else if(!(obj instanceof SelectionColumnMapping)) {
         return false;
      } else {
         SelectionColumns other = (SelectionColumns)obj;
         return Objects.equals(this.columnMappings, other.getMappings()) && Objects.equals(this.columnSpecifications, other.getColumnSpecifications());
      }
   }

   public int hashCode() {
      return Objects.hashCode(this.columnMappings);
   }

   public String toString() {
      return (String)this.columnMappings.asMap().entrySet().stream().map((entry) -> {
         return (String)(entry.getValue()).stream().map((colDef) -> {
            return colDef.name.toString();
         }).collect(Collectors.joining(", ", ((ColumnSpecification)entry.getKey()).name.toString() + ":[", "]"));
      }).collect(Collectors.joining(", ", (CharSequence)this.columnSpecifications.stream().map((colSpec) -> {
         return colSpec.name.toString();
      }).collect(Collectors.joining(", ", "{ Columns:[", "], Mappings:{")), "} }"));
   }
}
