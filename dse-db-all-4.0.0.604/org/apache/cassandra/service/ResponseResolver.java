package org.apache.cassandra.service;

import io.reactivex.Completable;
import io.reactivex.functions.Function;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ResponseResolver<T> {
   protected static final Logger logger = LoggerFactory.getLogger(ResponseResolver.class);
   final ReadCommand command;
   final ReadContext ctx;
   protected final Accumulator<Response<ReadResponse>> responses;

   ResponseResolver(ReadCommand command, ReadContext ctx, int maxResponseCount) {
      assert !command.isDigestQuery() : "Shouldn't create a response resolver with a digest command; cmd=" + command;

      this.command = command;
      this.ctx = ctx;
      this.responses = new Accumulator(maxResponseCount);
   }

   ConsistencyLevel consistency() {
      return this.ctx.consistencyLevel;
   }

   public abstract Flow<T> getData();

   public abstract Flow<T> resolve() throws DigestMismatchException;

   public abstract Completable compareResponses() throws DigestMismatchException;

   public abstract boolean isDataPresent();

   public void preprocess(Response<ReadResponse> message) {
      this.responses.add(message);
   }

   public Iterable<Response<ReadResponse>> getMessages() {
      return this.responses;
   }

   protected Flow<FlowableUnfilteredPartition> fromSingleResponse(ReadResponse response) {
      Flow<FlowableUnfilteredPartition> result = response.data(this.command);
      return this.ctx.readObserver == null?result:result.map((p) -> {
         this.ctx.readObserver.onPartition(p.partitionKey());
         if(!p.partitionLevelDeletion().isLive()) {
            this.ctx.readObserver.onPartitionDeletion(p.partitionLevelDeletion(), true);
         }

         if(!p.staticRow().isEmpty()) {
            this.ctx.readObserver.onRow(p.staticRow(), true);
         }

         return p.mapContent((u) -> {
            if(u.isRow()) {
               this.ctx.readObserver.onRow((Row)u, true);
            } else {
               this.ctx.readObserver.onRangeTombstoneMarker((RangeTombstoneMarker)u, true);
            }

            return u;
         });
      });
   }

   protected Flow<FlowablePartition> fromSingleResponseFiltered(ReadResponse response) {
      return FlowablePartitions.filterAndSkipEmpty(this.fromSingleResponse(response), this.command.nowInSec());
   }
}
