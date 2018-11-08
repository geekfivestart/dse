package org.apache.cassandra.cql3.statements;

public enum StatementType {
   INSERT {
      public boolean allowClusteringColumnSlices() {
         return false;
      }
   },
   UPDATE {
      public boolean allowClusteringColumnSlices() {
         return false;
      }
   },
   DELETE {
   },
   SELECT {
      public boolean allowPartitionKeyRanges() {
         return true;
      }

      public boolean allowNonPrimaryKeyInWhereClause() {
         return true;
      }

      public boolean allowUseOfSecondaryIndices() {
         return true;
      }
   };

   private StatementType() {
   }

   public boolean isInsert() {
      return this == INSERT;
   }

   public boolean isUpdate() {
      return this == UPDATE;
   }

   public boolean isDelete() {
      return this == DELETE;
   }

   public boolean isSelect() {
      return this == SELECT;
   }

   public boolean allowPartitionKeyRanges() {
      return false;
   }

   public boolean allowClusteringColumnSlices() {
      return true;
   }

   public boolean allowNonPrimaryKeyInWhereClause() {
      return false;
   }

   public boolean allowUseOfSecondaryIndices() {
      return false;
   }
}
