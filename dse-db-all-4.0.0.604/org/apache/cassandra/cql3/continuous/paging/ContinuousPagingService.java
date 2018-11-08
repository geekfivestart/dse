package org.apache.cassandra.cql3.continuous.paging;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.cql3.PagingResult;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.exceptions.ClientWriteException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.metrics.ContinuousPagingMetrics;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousPagingService {
   private static final Logger logger = LoggerFactory.getLogger(ContinuousPagingService.class);
   public static final ContinuousPagingMetrics metrics = new ContinuousPagingMetrics();
   private static final ConcurrentHashMap<ContinuousPagingService.SessionKey, ContinuousPagingService.ContinuousPagingSession> sessions = new ConcurrentHashMap();
   @VisibleForTesting
   static final AtomicIntegerArray numSessions = new AtomicIntegerArray(TPC.getNumCores());

   public ContinuousPagingService() {
   }

   public static ResultBuilder createSession(Selection.Selectors selectors, GroupMaker groupMaker, ResultSet.ResultMetadata resultMetadata, ContinuousPagingState continuousPagingState, QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException {
      assert options.continuousPagesRequested();

      ContinuousPagingService.SessionKey key = new ContinuousPagingService.SessionKey(queryState.getClientState().getRemoteAddress(), queryState.getStreamId());
      if(!canCreateSession(continuousPagingState)) {
         metrics.creationFailures.mark();
         metrics.tooManySessions.mark();
         long numSessions = liveSessions();
         if(logger.isDebugEnabled()) {
            logger.debug("Too many continuous paging sessions are already running: {}", Long.valueOf(numSessions));
         }

         throw RequestValidations.invalidRequest("Invalid request, too many continuous paging sessions are already running: %d", new Object[]{Long.valueOf(numSessions)});
      } else {
         if(logger.isTraceEnabled()) {
            logger.trace("Starting continuous paging session {} with paging options {}, local {}, total number of sessions running: {}", new Object[]{key, options.getPagingOptions(), Boolean.valueOf(continuousPagingState.executor.isLocalQuery()), Long.valueOf(liveSessions())});
         }

         ContinuousPagingService.ContinuousPagingSession session = new ContinuousPagingService.ContinuousPagingSession(selectors, groupMaker, resultMetadata, queryState, options, continuousPagingState, key);
         if(sessions.putIfAbsent(key, session) != null) {
            session.release();
            numSessions.decrementAndGet(session.coreId());
            metrics.creationFailures.mark();
            logger.error("Continuous paging session {} already exists", key);
            throw RequestValidations.invalidRequest("Invalid request, already executing continuous paging session %s", new Object[]{key});
         } else {
            return new ContinuousPagingService.ContinuousPagingSession.Builder(session, selectors, groupMaker);
         }
      }
   }

   private static boolean canCreateSession(ContinuousPagingState state) {
      int currentCore = TPC.bestTPCCore();

      Pair preferred;
      do {
         preferred = getPreferredCore(currentCore, state);
         if(((Integer)preferred.left).intValue() == -1) {
            logger.debug("Failed to find a core with fewer sessions than maximum allowed, current distrib: {}. max distrib: {}", numSessions, state.config.getMaxSessionsCoreDistribution(numSessions.length()));
            return false;
         }
      } while(!numSessions.compareAndSet(((Integer)preferred.left).intValue(), ((Integer)preferred.right).intValue(), ((Integer)preferred.right).intValue() + 1));

      if(logger.isTraceEnabled()) {
         logger.trace("Assigning core {} for next session, current distrib: {}, max distrib: {}", new Object[]{preferred.left, numSessions, state.config.getMaxSessionsCoreDistribution(numSessions.length())});
      }

      state.executor.setCoreId(((Integer)preferred.left).intValue());
      return true;
   }

   private static Pair<Integer, Integer> getPreferredCore(int currentCore, ContinuousPagingState state) {
      int currentSessions = numSessions.get(currentCore);
      Pair<Integer, Integer> ret = currentSessions < state.config.getMaxSessionsPerCore(numSessions.length(), currentCore)?Pair.create(Integer.valueOf(currentCore), Integer.valueOf(currentSessions)):Pair.create(Integer.valueOf(-1), Integer.valueOf(0));

      for(int i = 0; i < numSessions.length() && (((Integer)ret.right).intValue() != 0 || ((Integer)ret.left).intValue() == -1); ++i) {
         int sessions = numSessions.get(i);
         if(sessions < state.config.getMaxSessionsPerCore(numSessions.length(), i) && (sessions < ((Integer)ret.right).intValue() || ((Integer)ret.left).intValue() == -1)) {
            ret = Pair.create(Integer.valueOf(i), Integer.valueOf(sessions));
         }
      }

      return ret;
   }

   private static ContinuousPagingService.ContinuousPagingSession getSession(ContinuousPagingService.SessionKey key) {
      return (ContinuousPagingService.ContinuousPagingSession)sessions.get(key);
   }

   private static ContinuousPagingService.ContinuousPagingSession removeSession(ContinuousPagingService.SessionKey key) {
      ContinuousPagingService.ContinuousPagingSession ret = (ContinuousPagingService.ContinuousPagingSession)sessions.remove(key);
      if(ret != null) {
         numSessions.decrementAndGet(ret.coreId());
         if(logger.isTraceEnabled()) {
            logger.trace("Removed continuous paging session {}, {} sessions still running", key, Long.valueOf(liveSessions()));
         }
      }

      return ret;
   }

   public static long liveSessions() {
      long ret = 0L;

      for(int i = 0; i < numSessions.length(); ++i) {
         ret += (long)numSessions.get(i);
      }

      return ret;
   }

   public static long pendingPages() {
      return (long)((Integer)sessions.values().stream().map(ContinuousPagingService.ContinuousPagingSession::pendingPages).reduce(Integer::sum).orElseGet(() -> {
         return Integer.valueOf(0);
      })).intValue();
   }

   public static Single<Boolean> cancel(Single<QueryState> queryState, int streamId) {
      return queryState.flatMap((qs) -> {
         ContinuousPagingService.SessionKey key = new ContinuousPagingService.SessionKey(qs.getClientState().getRemoteAddress(), streamId);
         ContinuousPagingService.ContinuousPagingSession session = removeSession(key);
         if(session == null) {
            if(logger.isTraceEnabled()) {
               logger.trace("Cannot cancel continuous paging session {}: not found", key);
            }

            return Single.just(Boolean.valueOf(false));
         } else {
            if(logger.isTraceEnabled()) {
               logger.trace("Cancelling continuous paging session {}", key);
            }

            return TPCUtils.toSingle(session.cancel().thenCompose((ret) -> {
               return TPCUtils.completedFuture(Boolean.valueOf(true));
            })).timeout((long)session.config().cancel_timeout_sec, TimeUnit.SECONDS);
         }
      });
   }

   public static Single<Boolean> updateBackpressure(Single<QueryState> queryState, int streamId, int nextPages) {
      return queryState.map((qs) -> {
         ContinuousPagingService.SessionKey key = new ContinuousPagingService.SessionKey(qs.getClientState().getRemoteAddress(), streamId);
         if(nextPages <= 0) {
            throw RequestValidations.invalidRequest(String.format("Cannot update next_pages for continuous paging session %s, expected positive value but got %d", new Object[]{key, Integer.valueOf(nextPages)}));
         } else {
            ContinuousPagingService.ContinuousPagingSession session = getSession(key);
            if(session != null && session.state.get() == ContinuousPagingService.ContinuousPagingSession.State.RUNNING) {
               if(session.numPagesRequested == 2147483647) {
                  logger.warn("Cannot update next_pages for continuous paging session {}, numPagesRequested already set to maximum", key);
                  return Boolean.valueOf(false);
               } else {
                  return Boolean.valueOf(session.updateBackpressure(nextPages));
               }
            } else {
               if(logger.isTraceEnabled()) {
                  logger.trace("Cannot update next_pages for continuous paging session {}: not found or no longer running", key);
               }

               return Boolean.valueOf(false);
            }
         }
      });
   }

   @VisibleForTesting
   static final class ContinuousPagingSession {
      private final ResultSet.ResultMetadata resultMetaData;
      private final QueryState queryState;
      private final Selection.Selectors selectors;
      private final GroupMaker groupMaker;
      private final QueryOptions options;
      private final ContinuousPageWriter pageWriter;
      private final ContinuousPagingState continuousPagingState;
      private final QueryOptions.PagingOptions pagingOptions;
      private final ContinuousPagingService.SessionKey key;
      private int avgRowSize;
      @VisibleForTesting
      int numPagesSent;
      private int numPagesRequested;
      private ContinuousPagingService.ContinuousPagingSession.Page currentPage;
      private final AtomicReference<ContinuousPagingService.ContinuousPagingSession.State> state;
      private volatile long paused;
      @Nullable
      private volatile PagingState pagingState;

      ContinuousPagingSession(Selection.Selectors selectors, GroupMaker groupMaker, ResultSet.ResultMetadata resultMetadata, QueryState queryState, QueryOptions options, ContinuousPagingState continuousPagingState, ContinuousPagingService.SessionKey key) {
         this.selectors = selectors;
         this.groupMaker = groupMaker;
         this.resultMetaData = resultMetadata;
         this.queryState = queryState;
         this.options = options;
         this.continuousPagingState = continuousPagingState;
         this.key = key;
         this.pagingOptions = options.getPagingOptions();
         this.numPagesSent = 0;
         this.numPagesRequested = this.pagingOptions.nextPages() <= 0?2147483647:this.pagingOptions.nextPages();
         this.state = new AtomicReference(ContinuousPagingService.ContinuousPagingSession.State.RUNNING);
         this.paused = -1L;
         this.pageWriter = new ContinuousPageWriter(continuousPagingState.channel, this.pagingOptions.maxPagesPerSecond(), continuousPagingState.config.max_session_pages);
         this.avgRowSize = continuousPagingState.averageRowSize;
         this.allocatePage(1);
      }

      int coreId() {
         return this.continuousPagingState.executor.coreId();
      }

      int pendingPages() {
         return this.pageWriter.pendingPages();
      }

      public CompletableFuture<Void> cancel() {
         if(this.state.compareAndSet(ContinuousPagingService.ContinuousPagingSession.State.RUNNING, ContinuousPagingService.ContinuousPagingSession.State.CANCEL_REQUESTED)) {
            Message.Response err = ErrorMessage.fromException(new RuntimeException("Session cancelled by the user"), (throwable) -> {
               return true;
            });
            this.pageWriter.cancel(ContinuousPagingService.ContinuousPagingSession.Page.makeFrame(err, err.type.codec, this.options.getProtocolVersion(), this.queryState.getStreamId()));
            ContinuousPagingService.logger.trace("Continuous paging session {} cancelled by the user", this.key);
         } else if(ContinuousPagingService.logger.isTraceEnabled()) {
            ContinuousPagingService.logger.trace("Could not cancel continuous paging session {}, not running ({})", this.key, this.state.get());
         }

         return this.pageWriter.completionFuture();
      }

      boolean updateBackpressure(int nextPages) {
         assert nextPages > 0 : "nextPages should be positive";

         int old = this.numPagesRequested;
         this.numPagesRequested += nextPages;
         if(this.numPagesRequested <= old) {
            ContinuousPagingService.logger.warn("Tried to increase numPagesRequested from {} by {} but got {} for session {}, setting it to {}", new Object[]{Integer.valueOf(old), Integer.valueOf(nextPages), Integer.valueOf(this.numPagesRequested), this.key, Integer.valueOf(2147483647)});
            this.numPagesRequested = 2147483647;
            return false;
         } else {
            if(ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Updated numPagesRequested to {} for continuous paging session {}", Integer.valueOf(this.numPagesRequested), this.key);
            }

            this.continuousPagingState.executor.schedule(this::maybeResume);
            return true;
         }
      }

      public ContinuousPagingConfig config() {
         return this.continuousPagingState.config;
      }

      private void allocatePage(int seqNo) {
         if(this.currentPage == null) {
            int maxPageSize = this.maxPageSize();
            int bufferSize = Math.min(maxPageSize, this.pagingOptions.pageSize().inEstimatedBytes(this.avgRowSize) + this.safePageMargin());
            if(ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Allocating page with buffer size {}, avg row size {} for {}", new Object[]{Integer.valueOf(bufferSize), Integer.valueOf(this.avgRowSize), this.key});
            }

            this.currentPage = new ContinuousPagingService.ContinuousPagingSession.Page(bufferSize, this.resultMetaData, this.queryState, this.options, this.key, seqNo, maxPageSize);
         } else {
            if(ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Reusing page with buffer size {}, avg row size {} for {}", new Object[]{Integer.valueOf(this.currentPage.buf.capacity()), Integer.valueOf(this.avgRowSize), this.key});
            }

            this.currentPage.reuse(seqNo);
         }

      }

      int maxPageSize() {
         return this.continuousPagingState.config.max_page_size_mb * 1024 * 1024;
      }

      private int safePageMargin() {
         return 2 * this.avgRowSize;
      }

      private void processPage(boolean last, boolean nextRowPending) {
         if(this.state.get() != ContinuousPagingService.ContinuousPagingSession.State.CANCEL_REQUESTED) {
            assert !this.currentPage.isEmpty() || last;

            this.avgRowSize = this.currentPage.avgRowSize(this.avgRowSize);
            this.pagingState = this.continuousPagingState.executor.state(nextRowPending);
            if(ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Sending page nr. {} with {} rows, average row size: {}, last: {}, nextRowPending: {}, pagingState {} for session {}", new Object[]{Integer.valueOf(this.numPagesSent + 1), Integer.valueOf(this.currentPage.numRows), Integer.valueOf(this.avgRowSize), Boolean.valueOf(last), Boolean.valueOf(nextRowPending), this.pagingState, this.key});
            }

            PagingResult pagingResult = new PagingResult(this.pagingState, this.currentPage.seqNo, last);
            this.pageWriter.sendPage(this.currentPage.makeFrame(pagingResult), !last);
            ++this.numPagesSent;
         }
      }

      boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending) {
         if(this.state.get() == ContinuousPagingService.ContinuousPagingSession.State.CANCEL_REQUESTED) {
            this.stop();
            return false;
         } else if(this.currentPage == null) {
            assert this.state.get() == ContinuousPagingService.ContinuousPagingSession.State.STOPPED : "Invalid state with null page: " + this.state.get();

            return false;
         } else {
            this.currentPage.addRow(row);
            boolean mustSendPage = !nextRowPending || this.pageCompleted(this.currentPage.numRows, this.currentPage.size(), this.avgRowSize) || this.pageIsCloseToMax();
            if(!mustSendPage) {
               return true;
            } else {
               boolean isLastPage = !nextRowPending || this.isLastPage(this.currentPage.seqNo());
               if(isLastPage) {
                  ContinuousPagingService.removeSession(this.key);
               }

               this.processPage(isLastPage, nextRowPending);
               if(this.numPagesSent == 2147483647) {
                  this.sendError(new ClientWriteException(String.format("Reached maximum number of pages (%d), stopping session to avoid overflow", new Object[]{Integer.valueOf(this.numPagesSent)})));
               } else if(!isLastPage && this.state.get() == ContinuousPagingService.ContinuousPagingSession.State.RUNNING) {
                  this.allocatePage(this.currentPage.seqNo() + 1);
                  this.maybePause();
               } else {
                  this.stop();
               }

               return this.state.get() == ContinuousPagingService.ContinuousPagingSession.State.RUNNING;
            }
         }
      }

      void maybePause() {
         long startedAtMillis = this.continuousPagingState.executor.localStartTimeInMillis();
         ContinuousBackPressureException ret;
         if(!this.pageWriter.hasSpace()) {
            ret = new ContinuousBackPressureException(String.format("Continuous paging queue is full (%d pages in the queue)", new Object[]{Integer.valueOf(this.pageWriter.pendingPages())}));
         } else if(this.numPagesSent >= this.numPagesRequested) {
            ret = new ContinuousBackPressureException(String.format("Continuous paging backpressure was triggered, requested %d sent %d", new Object[]{Integer.valueOf(this.numPagesRequested), Integer.valueOf(this.numPagesSent)}));
         } else {
            if(startedAtMillis <= 0L || this.continuousPagingState.timeSource.currentTimeMillis() - startedAtMillis < (long)this.continuousPagingState.config.max_local_query_time_ms) {
               return;
            }

            ret = new ContinuousBackPressureException(String.format("Locally optimized query running longer than %d milliseconds", new Object[]{Integer.valueOf(this.continuousPagingState.config.max_local_query_time_ms)}));
         }

         if(ContinuousPagingService.logger.isTraceEnabled()) {
            ContinuousPagingService.logger.trace("Pausing session {}: {}", this.key, ret.getMessage());
         }

         this.paused = this.continuousPagingState.timeSource.nanoTime();
         ContinuousPagingService.metrics.serverBlocked.inc();
         this.continuousPagingState.executor.schedule(this::maybeResume, (long)this.continuousPagingState.config.paused_check_interval_ms, TimeUnit.MILLISECONDS);
         throw ret;
      }

      private void maybeResume() {
         if(this.paused != -1L && this.state.get() != ContinuousPagingService.ContinuousPagingSession.State.STOPPED) {
            long now = this.continuousPagingState.timeSource.nanoTime();
            boolean canResume = this.numPagesRequested == 2147483647?this.pageWriter.halfQueueAvailable():this.pageWriter.hasSpace() && this.numPagesSent < this.numPagesRequested;
            if(canResume && ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Resuming session {}, pages requested: {}, pages sent: {}, queue size: {}, paging state {}", new Object[]{this.key, Integer.valueOf(this.numPagesRequested), Integer.valueOf(this.numPagesSent), Integer.valueOf(this.pageWriter.pendingPages()), this.pagingState});
            }

            if(!canResume) {
               if(now - this.paused >= TimeUnit.SECONDS.toNanos((long)this.continuousPagingState.config.client_timeout_sec)) {
                  ContinuousPagingService.metrics.serverBlockedLatency.addNano(now - this.paused);
                  this.sendError(new ClientWriteException(String.format("Paused for longer than %d seconds and unable to write pages to client", new Object[]{Integer.valueOf(this.continuousPagingState.config.client_timeout_sec)})));
               } else {
                  this.continuousPagingState.executor.schedule(this::maybeResume, (long)this.continuousPagingState.config.paused_check_interval_ms, TimeUnit.MILLISECONDS);
               }
            } else {
               ContinuousPagingService.metrics.serverBlockedLatency.addNano(now - this.paused);
               this.paused = -1L;
               this.continuousPagingState.executor.schedule(this.pagingState, new ContinuousPagingService.ContinuousPagingSession.Builder(this, this.selectors, this.groupMaker));
            }

         }
      }

      private boolean pageCompleted(int numRows, int size, int rowSize) {
         return this.pagingOptions.pageSize().isComplete(numRows, size + rowSize - 1);
      }

      private boolean isLastPage(int pageNo) {
         return pageNo >= this.pagingOptions.maxPages();
      }

      private boolean pageIsCloseToMax() {
         return this.currentPage != null && this.maxPageSize() - this.currentPage.size() < this.safePageMargin();
      }

      private void stop() {
         ContinuousPagingService.ContinuousPagingSession.State current = (ContinuousPagingService.ContinuousPagingSession.State)this.state.get();

         assert current == ContinuousPagingService.ContinuousPagingSession.State.RUNNING || current == ContinuousPagingService.ContinuousPagingSession.State.CANCEL_REQUESTED : "Invalid state when stopping: " + current;

         if(this.state.compareAndSet(current, ContinuousPagingService.ContinuousPagingSession.State.STOPPED)) {
            if(ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Continuous paging session {} stopped, previously in state {}", this.key, current);
            }

            this.release();
         } else {
            ContinuousPagingService.logger.error("Failed to stop session {} ({})", this.key, this.state.get());
         }

      }

      private void release() {
         if(this.currentPage != null) {
            this.currentPage.release();
            this.currentPage = null;
         }

      }

      boolean resultIsEmpty() {
         return this.numPagesSent == 0 && this.currentPage != null && this.currentPage.isEmpty();
      }

      void sendError(Throwable error) {
         if(ContinuousPagingService.logger.isTraceEnabled()) {
            ContinuousPagingService.logger.trace("Sending error {}/{} for session {}", new Object[]{error.getClass(), error.getMessage(), this.key});
         }

         if(error instanceof ClientWriteException) {
            ContinuousPagingService.metrics.clientWriteExceptions.mark();
         } else {
            ContinuousPagingService.metrics.failures.mark();
         }

         if(this.currentPage != null) {
            this.stop();
         }

         ContinuousPagingService.removeSession(this.key);
         Message.Response err = ErrorMessage.fromException(error);
         this.pageWriter.sendError(ContinuousPagingService.ContinuousPagingSession.Page.makeFrame(err, err.type.codec, this.options.getProtocolVersion(), this.queryState.getStreamId()));
      }

      void sendLastPage() {
         if(this.state.get() != ContinuousPagingService.ContinuousPagingSession.State.STOPPED) {
            ContinuousPagingService.removeSession(this.key);
            this.processPage(true, false);
            this.stop();
         }

      }

      private static enum State {
         RUNNING,
         CANCEL_REQUESTED,
         STOPPED;

         private State() {
         }
      }

      static class Builder extends ResultBuilder {
         @VisibleForTesting
         final ContinuousPagingService.ContinuousPagingSession session;

         Builder(ContinuousPagingService.ContinuousPagingSession session, Selection.Selectors selectors, GroupMaker groupMaker) {
            super(selectors, groupMaker);
            this.session = session;
         }

         public boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending) {
            return this.session.onRowCompleted(row, nextRowPending);
         }

         public boolean resultIsEmpty() {
            return this.session.resultIsEmpty();
         }

         public void complete(Throwable error) {
            this.session.sendError(error);
         }

         public void complete() {
            if(ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Completing continuous paging session {}", this.session.key);
            }

            super.complete();
            this.session.sendLastPage();
            long duration = this.session.continuousPagingState.timeSource.nanoTime() - this.session.continuousPagingState.executor.queryStartTimeInNanos();
            ContinuousPagingService.metrics.addTotalDuration(this.session.continuousPagingState.executor.isLocalQuery(), duration);
            if(ContinuousPagingService.logger.isTraceEnabled()) {
               ContinuousPagingService.logger.trace("Completed continuous paging session {} after {} milliseconds", this.session.key, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(duration)));
            }

         }

         public String toString() {
            return String.format("Continuous paging session %s", new Object[]{this.session.key});
         }
      }

      static class Page {
         final ResultSet.ResultMetadata metadata;
         final QueryState state;
         final QueryOptions.PagingOptions pagingOptions;
         final ProtocolVersion version;
         final ContinuousPagingService.SessionKey sessionKey;
         ByteBuf buf;
         int numRows;
         int seqNo;
         int maxPageSize;

         Page(int bufferSize, ResultSet.ResultMetadata metadata, QueryState state, QueryOptions options, ContinuousPagingService.SessionKey sessionKey, int seqNo, int maxPageSize) {
            this.metadata = metadata;
            this.state = state;
            this.pagingOptions = options.getPagingOptions();
            this.version = options.getProtocolVersion();
            this.sessionKey = sessionKey;
            this.buf = CBUtil.allocator.buffer(bufferSize);
            this.seqNo = seqNo;
            this.maxPageSize = maxPageSize;
         }

         Frame makeFrame(PagingResult pagingResult) {
            this.metadata.setPagingResult(pagingResult);
            ContinuousPagingService.ContinuousPagingSession.EncodedPage response = new ContinuousPagingService.ContinuousPagingSession.EncodedPage(this.metadata, this.numRows, this.buf);
            response.setWarnings(ClientWarn.instance.getWarnings());
            if(Tracing.isTracing()) {
               response.setTracingId(Tracing.instance.getSessionId());
            }

            return makeFrame(response, ContinuousPagingService.ContinuousPagingSession.EncodedPage.codec, this.version, this.state.getStreamId());
         }

         static <M extends Message.Response> Frame makeFrame(M response, Message.Codec codec, ProtocolVersion version, int streamId) {
            response.setStreamId(streamId);
            int messageSize = codec.encodedSize(response, version);
            Frame frame = Message.ProtocolEncoder.makeFrame(response, messageSize, version);
            codec.encode(response, frame.body, version);
            return frame;
         }

         void addRow(List<ByteBuffer> row) {
            int prevWriteIndex = this.buf.writerIndex();
            boolean ret = ResultSet.codec.encodeRow(row, this.metadata, this.buf, true);
            if(ret) {
               ++this.numRows;
            } else {
               this.buf.writerIndex(prevWriteIndex);
               int rowSize = ResultSet.codec.encodedRowSize(row, this.metadata);
               int bufferSize = Math.max(this.buf.readableBytes() + rowSize, Math.min(this.buf.capacity() * 2, this.maxPageSize));
               if(ContinuousPagingService.logger.isTraceEnabled()) {
                  ContinuousPagingService.logger.trace("Reallocating page buffer from {}/{} to {} for row size {} - {}", new Object[]{Integer.valueOf(this.buf.readableBytes()), Integer.valueOf(this.buf.capacity()), Integer.valueOf(bufferSize), Integer.valueOf(rowSize), this.sessionKey});
               }

               ByteBuf old = this.buf;

               try {
                  this.buf = null;
                  this.buf = CBUtil.allocator.buffer(bufferSize);
                  this.buf.writeBytes(old);
                  ResultSet.codec.encodeRow(row, this.metadata, this.buf, false);
                  ++this.numRows;
               } finally {
                  old.release();
               }

            }
         }

         int size() {
            return this.buf.readableBytes();
         }

         boolean isEmpty() {
            return this.numRows == 0;
         }

         void reuse(int seqNo) {
            this.numRows = 0;
            this.seqNo = seqNo;
            this.buf.clear();
         }

         void release() {
            this.buf.release();
            this.buf = null;
         }

         int seqNo() {
            return this.seqNo;
         }

         int avgRowSize(int current) {
            if(this.buf != null && this.numRows != 0) {
               int avg = this.buf.readableBytes() / this.numRows;
               return (avg + current) / 2;
            } else {
               return current;
            }
         }

         public String toString() {
            return String.format("[Page rows: %d]", new Object[]{Integer.valueOf(this.numRows)});
         }
      }

      private static class EncodedPage extends ResultMessage {
         final ResultSet.ResultMetadata metadata;
         final int numRows;
         final ByteBuf buff;
         public static final Message.Codec<ContinuousPagingService.ContinuousPagingSession.EncodedPage> codec = new Message.Codec<ContinuousPagingService.ContinuousPagingSession.EncodedPage>() {
            public ContinuousPagingService.ContinuousPagingSession.EncodedPage decode(ByteBuf body, ProtocolVersion version) {
               assert false : "should never be called";

               return null;
            }

            public void encode(ContinuousPagingService.ContinuousPagingSession.EncodedPage msg, ByteBuf dest, ProtocolVersion version) {
               dest.writeInt(msg.kind.id);
               ResultSet.codec.encodeHeader(msg.metadata, dest, msg.numRows, version);
               dest.writeBytes(msg.buff);
            }

            public int encodedSize(ContinuousPagingService.ContinuousPagingSession.EncodedPage msg, ProtocolVersion version) {
               return 4 + ResultSet.codec.encodedHeaderSize(msg.metadata, version) + msg.buff.readableBytes();
            }
         };

         private EncodedPage(ResultSet.ResultMetadata metadata, int numRows, ByteBuf buff) {
            super(ResultMessage.Kind.ROWS);
            this.metadata = metadata;
            this.numRows = numRows;
            this.buff = buff;
         }

         public String toString() {
            return String.format("ENCODED PAGE (%d ROWS)", new Object[]{Integer.valueOf(this.numRows)});
         }
      }
   }

   private static final class SessionKey {
      private final InetSocketAddress address;
      private final int streamId;

      SessionKey(InetSocketAddress address, int streamId) {
         this.address = address;
         this.streamId = streamId;
      }

      public boolean equals(Object other) {
         if(this == other) {
            return true;
         } else if(!(other instanceof ContinuousPagingService.SessionKey)) {
            return false;
         } else {
            ContinuousPagingService.SessionKey that = (ContinuousPagingService.SessionKey)other;
            return Objects.equals(this.address, that.address) && this.streamId == that.streamId;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.address, Integer.valueOf(this.streamId)});
      }

      public String toString() {
         return String.format("%s/%d", new Object[]{this.address, Integer.valueOf(this.streamId)});
      }
   }
}
