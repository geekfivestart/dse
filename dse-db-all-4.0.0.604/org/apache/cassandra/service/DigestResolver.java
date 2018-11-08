package org.apache.cassandra.service;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadReconciliationObserver;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.flow.Flow;

public class DigestResolver extends ResponseResolver<FlowablePartition> {
   volatile Response<ReadResponse> dataResponse;

   DigestResolver(ReadCommand command, ReadContext params, int maxResponseCount) {
      super(command, params, maxResponseCount);
   }

   public void preprocess(Response<ReadResponse> message) {
      super.preprocess(message);
      if(this.dataResponse == null && !((ReadResponse)message.payload()).isDigestResponse()) {
         this.dataResponse = message;
      }

   }

   public Flow<FlowablePartition> getData() {
      assert this.isDataPresent();

      if(this.ctx.readObserver != null) {
         this.ctx.readObserver.onDigestMatch();
      }

      return this.fromSingleResponseFiltered((ReadResponse)this.dataResponse.payload());
   }

   public Flow<FlowablePartition> resolve() throws DigestMismatchException {
      if(this.responses.size() == 1) {
         return this.getData();
      } else {
         if(logger.isTraceEnabled()) {
            logger.trace("resolving {} responses", Integer.valueOf(this.responses.size()));
         }

         return Flow.concat(this.compareResponses(), this.fromSingleResponseFiltered((ReadResponse)this.dataResponse.payload()));
      }
   }

   public Completable compareResponses() throws DigestMismatchException {
      long start = System.nanoTime();
      Completable pipeline = Single.concat(Iterables.transform(this.responses, (response) -> {
         return ((ReadResponse)response.payload()).digest(this.command);
      })).reduce((prev, digest) -> {
         if(prev.equals(digest)) {
            return digest;
         } else {
            if(this.ctx.readObserver != null) {
               this.ctx.readObserver.onDigestMismatch();
            }

            throw new DigestMismatchException(this.command, prev, digest);
         }
      }).ignoreElement();
      if(logger.isTraceEnabled()) {
         pipeline = pipeline.doFinally(() -> {
            logger.trace("resolve: {} ms.", Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
         });
      }

      if(this.ctx.readObserver != null) {
         ReadReconciliationObserver var10001 = this.ctx.readObserver;
         this.ctx.readObserver.getClass();
         pipeline = pipeline.doOnComplete(var10001::onDigestMatch);
      }

      return pipeline;
   }

   public boolean isDataPresent() {
      return this.dataResponse != null;
   }
}
