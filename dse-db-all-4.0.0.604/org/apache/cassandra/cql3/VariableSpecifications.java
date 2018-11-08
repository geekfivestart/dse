package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class VariableSpecifications {
   private final List<ColumnIdentifier> variableNames;
   private final ColumnSpecification[] specs;
   private final ColumnMetadata[] targetColumns;

   public VariableSpecifications(List<ColumnIdentifier> variableNames) {
      this.variableNames = variableNames;
      this.specs = new ColumnSpecification[variableNames.size()];
      this.targetColumns = new ColumnMetadata[variableNames.size()];
   }

   public static VariableSpecifications empty() {
      return new VariableSpecifications(Collections.emptyList());
   }

   public int size() {
      return this.variableNames.size();
   }

   public List<ColumnSpecification> getSpecifications() {
      return Arrays.asList(this.specs);
   }

   public short[] getPartitionKeyBindIndexes(TableMetadata metadata) {
      short[] partitionKeyPositions = new short[metadata.partitionKeyColumns().size()];
      boolean[] set = new boolean[partitionKeyPositions.length];

      for(int i = 0; i < this.targetColumns.length; ++i) {
         ColumnMetadata targetColumn = this.targetColumns[i];
         if(targetColumn != null && targetColumn.isPartitionKey()) {
            assert targetColumn.ksName.equals(metadata.keyspace) && targetColumn.cfName.equals(metadata.name);

            partitionKeyPositions[targetColumn.position()] = (short)i;
            set[targetColumn.position()] = true;
         }
      }

      boolean[] var8 = set;
      int var9 = set.length;

      for(int var6 = 0; var6 < var9; ++var6) {
         boolean b = var8[var6];
         if(!b) {
            return null;
         }
      }

      return partitionKeyPositions;
   }

   public void add(int bindIndex, ColumnSpecification spec) {
      if(spec instanceof ColumnMetadata) {
         this.targetColumns[bindIndex] = (ColumnMetadata)spec;
      }

      ColumnIdentifier bindMarkerName = (ColumnIdentifier)this.variableNames.get(bindIndex);
      if(bindMarkerName != null) {
         spec = new ColumnSpecification(spec.ksName, spec.cfName, bindMarkerName, spec.type);
      }

      this.specs[bindIndex] = spec;
   }

   public String toString() {
      return Arrays.toString(this.specs);
   }
}
