package org.apache.cassandra.db;

import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadExecutionController implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(ReadExecutionController.class);
   private final OpOrder baseOp;
   private volatile OpOrder.Group baseOpGroup;
   private final TableMetadata baseMetadata;
   private final ReadExecutionController indexController;
   private final OpOrder writeOp;
   private volatile OpOrder.Group writeOpGroup;
   private boolean closed;

   private ReadExecutionController(OpOrder baseOp, TableMetadata baseMetadata, ReadExecutionController indexController, OpOrder writeOp) {
      assert baseOp == null == (baseMetadata == null);

      this.baseOp = baseOp;
      this.baseMetadata = baseMetadata;
      this.indexController = indexController;
      this.writeOp = writeOp;
      this.closed = false;
   }

   public ReadExecutionController indexReadController() {
      return this.indexController;
   }

   public OpOrder.Group writeOpOrderGroup() {
      assert this.writeOpGroup != null;

      return this.writeOpGroup;
   }

   public boolean startIfValid(ColumnFamilyStore cfs) {
      if(!this.closed && cfs.metadata.id.equals(this.baseMetadata.id) && this.baseOp != null) {
         if(this.baseOpGroup == null) {
            this.baseOpGroup = this.baseOp.start();
            if(this.writeOp != null) {
               this.writeOpGroup = this.writeOp.start();
            }
         }

         return true;
      } else {
         return false;
      }
   }

   public static ReadExecutionController empty() {
      return new ReadExecutionController((OpOrder)null, (TableMetadata)null, (ReadExecutionController)null, (OpOrder)null);
   }

   public static ReadExecutionController forCommand(ReadCommand command) {
      ColumnFamilyStore baseCfs = Keyspace.openAndGetStore(command.metadata());
      ColumnFamilyStore indexCfs = maybeGetIndexCfs(baseCfs, command);
      if(indexCfs == null) {
         return new ReadExecutionController(baseCfs.readOrdering, baseCfs.metadata(), (ReadExecutionController)null, (OpOrder)null);
      } else {
         OpOrder baseOp = null;
         OpOrder writeOp = null;
         ReadExecutionController indexController = null;

         try {
            baseOp = baseCfs.readOrdering;
            indexController = new ReadExecutionController(indexCfs.readOrdering, indexCfs.metadata(), (ReadExecutionController)null, (OpOrder)null);
            writeOp = Keyspace.writeOrder;
            return new ReadExecutionController(baseOp, baseCfs.metadata(), indexController, writeOp);
         } catch (RuntimeException var7) {
            if(indexController != null) {
               indexController.close();
            }

            throw var7;
         }
      }
   }

   private static ColumnFamilyStore maybeGetIndexCfs(ColumnFamilyStore baseCfs, ReadCommand command) {
      Index index = command.getIndex(baseCfs);
      return index == null?null:(ColumnFamilyStore)index.getBackingTable().orElse((Object)null);
   }

   public TableMetadata metadata() {
      return this.baseMetadata;
   }

   public void close() {
      if(!this.closed) {
         this.closed = true;
         Throwable fail = null;
         fail = Throwables.closeNonNull(fail, (AutoCloseable)this.baseOpGroup);
         fail = Throwables.closeNonNull(fail, (AutoCloseable)this.indexController);
         fail = Throwables.closeNonNull(fail, (AutoCloseable)this.writeOpGroup);
         if(fail != null) {
            JVMStabilityInspector.inspectThrowable(fail);
            logger.error("Failed to close ReadExecutionController: {}", fail);
         }

      }
   }
}
