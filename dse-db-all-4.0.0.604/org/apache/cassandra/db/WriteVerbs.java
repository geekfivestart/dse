package org.apache.cassandra.db;

import com.google.common.base.Throwables;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchRemove;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.DroppingResponseException;
import org.apache.cassandra.net.ErrorHandler;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteVerbs extends VerbGroup<WriteVerbs.WriteVersion> {
   private static final Logger logger = LoggerFactory.getLogger(ReadVerbs.class);
   public final Verb.AckedRequest<Mutation> WRITE;
   public final Verb.AckedRequest<Mutation> VIEW_WRITE;
   public final Verb.AckedRequest<CounterMutation> COUNTER_FORWARDING;
   public final Verb.AckedRequest<Mutation> READ_REPAIR;
   public final Verb.AckedRequest<Batch> BATCH_STORE;
   public final Verb.OneWay<BatchRemove> BATCH_REMOVE;
   private static final VerbHandlers.AckedRequest<Mutation> WRITE_HANDLER = (from, mutation) -> {
      return mutation.applyFuture();
   };
   private static final VerbHandlers.AckedRequest<CounterMutation> COUNTER_FORWARDING_HANDLER = (from, mutation) -> {
      long queryStartNanoTime = System.nanoTime();
      logger.trace("Applying forwarded {}", mutation);
      return StorageProxy.applyCounterMutationOnLeader(mutation, queryStartNanoTime).exceptionally((t) -> {
         if(t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
         }

         if(t instanceof RequestTimeoutException) {
            throw new DroppingResponseException();
         } else if(!(t instanceof RequestExecutionException) && !(t instanceof InternalRequestExecutionException)) {
            throw Throwables.propagate(t);
         } else {
            throw new CounterForwardingException(t);
         }
      });
   };
   private static final VerbHandlers.AckedRequest<Batch> BATCH_WRITE_HANDLER = (from, batch) -> {
      return TPCUtils.toFuture(BatchlogManager.store(batch));
   };
   private static final VerbHandlers.OneWay<BatchRemove> BATCH_REMOVE_HANDLER = (from, batchRemove) -> {
      TPCUtils.toFuture(BatchlogManager.remove(batchRemove.id));
   };

   public WriteVerbs(Verbs.Group id) {
      super(id, false, WriteVerbs.WriteVersion.class);
      VerbGroup<WriteVerbs.WriteVersion>.RegistrationHelper helper = this.helper();
      this.WRITE = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("WRITE", Mutation.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).droppedGroup(DroppedMessages.Group.MUTATION)).withBackPressure()).handler(WRITE_HANDLER);
      this.VIEW_WRITE = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("VIEW_WRITE", Mutation.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).droppedGroup(DroppedMessages.Group.VIEW_MUTATION)).withBackPressure()).handler(WRITE_HANDLER);
      this.COUNTER_FORWARDING = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("COUNTER_FORWARDING", CounterMutation.class).timeout(DatabaseDescriptor::getCounterWriteRpcTimeout)).droppedGroup(DroppedMessages.Group.COUNTER_MUTATION)).withBackPressure()).withErrorHandler((err) -> {
         if(err.reason == RequestFailureReason.COUNTER_FORWARDING_FAILURE) {
            ErrorHandler.noSpamLogger.debug(err.getMessage(), new Object[0]);
         } else {
            ErrorHandler.DEFAULT.handleError(err);
         }

      })).handler(COUNTER_FORWARDING_HANDLER);
      this.READ_REPAIR = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("READ_REPAIR", Mutation.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).droppedGroup(DroppedMessages.Group.READ_REPAIR)).handler(WRITE_HANDLER);
      this.BATCH_STORE = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("BATCH_STORE", Batch.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).droppedGroup(DroppedMessages.Group.BATCH_STORE)).withBackPressure()).handler(BATCH_WRITE_HANDLER);
      this.BATCH_REMOVE = helper.oneWay("BATCH_REMOVE", BatchRemove.class).handler(BATCH_REMOVE_HANDLER);
   }

   public static enum WriteVersion implements Version<WriteVerbs.WriteVersion> {
      OSS_30(EncodingVersion.OSS_30);

      public final EncodingVersion encodingVersion;

      private WriteVersion(EncodingVersion encodingVersion) {
         this.encodingVersion = encodingVersion;
      }

      public static <T> Versioned<WriteVerbs.WriteVersion, T> versioned(Function<WriteVerbs.WriteVersion, ? extends T> function) {
         return new Versioned(WriteVerbs.WriteVersion.class, function);
      }
   }
}
