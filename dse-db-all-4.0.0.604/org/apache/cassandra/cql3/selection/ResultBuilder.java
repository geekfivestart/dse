package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.rows.Cell;

public abstract class ResultBuilder {
   private final Selection.Selectors selectors;
   private final GroupMaker groupMaker;
   private Selector.InputRow inputRow;
   protected boolean completed;

   protected ResultBuilder(Selection.Selectors selectors, GroupMaker groupMaker) {
      this.selectors = selectors;
      this.groupMaker = groupMaker;
   }

   public void add(ByteBuffer v) {
      this.inputRow.add(v);
   }

   public void add(Cell c, int nowInSec) {
      this.inputRow.add(c, nowInSec);
   }

   public void newRow(DecoratedKey partitionKey, Clustering clustering) {
      boolean isNewAggregate = this.groupMaker == null || this.groupMaker.isNewGroup(partitionKey, clustering);
      if(this.inputRow != null) {
         this.selectors.addInputRow(this.inputRow);
         if(isNewAggregate) {
            boolean res = this.onRowCompleted(this.selectors.getOutputRow(), true);
            this.inputRow.reset(!this.selectors.hasProcessing());
            this.selectors.reset();
            if(!res) {
               this.complete();
            }
         } else {
            this.inputRow.reset(!this.selectors.hasProcessing());
         }
      } else {
         this.inputRow = this.selectors.newInputRow();
      }

   }

   public void complete(Throwable error) {
      this.complete();
   }

   public void complete() {
      if(!this.completed) {
         if(this.inputRow != null) {
            this.selectors.addInputRow(this.inputRow);
            this.onRowCompleted(this.selectors.getOutputRow(), false);
            this.inputRow.reset(!this.selectors.hasProcessing());
            this.selectors.reset();
         }

         if(this.resultIsEmpty() && this.groupMaker != null && this.groupMaker.returnAtLeastOneRow()) {
            this.onRowCompleted(this.selectors.getOutputRow(), false);
         }

         this.completed = true;
      }
   }

   public boolean isCompleted() {
      return this.completed;
   }

   public abstract boolean onRowCompleted(List<ByteBuffer> var1, boolean var2);

   public abstract boolean resultIsEmpty();
}
