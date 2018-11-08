package org.apache.cassandra.cql3.selection;

import com.google.common.collect.Multimap;
import java.util.List;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.schema.ColumnMetadata;

public interface SelectionColumns {
   List<ColumnSpecification> getColumnSpecifications();

   Multimap<ColumnSpecification, ColumnMetadata> getMappings();
}
