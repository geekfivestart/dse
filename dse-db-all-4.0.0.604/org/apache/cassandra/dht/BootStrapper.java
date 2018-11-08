package org.apache.cassandra.dht;

import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifierSupport;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootStrapper extends ProgressEventNotifierSupport {
   private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);
   protected final InetAddress address;
   protected final Collection<Token> tokens;
   protected final TokenMetadata tokenMetadata;

   public BootStrapper(InetAddress address, Collection<Token> tokens, TokenMetadata tmd) {
      assert address != null;

      assert tokens != null && !tokens.isEmpty();

      this.address = address;
      this.tokens = tokens;
      this.tokenMetadata = tmd;
   }

   public ListenableFuture<StreamState> bootstrap(StreamStateStore stateStore, boolean useStrictConsistency) {
      StreamingOptions options = StreamingOptions.forBootStrap(this.tokenMetadata.cloneOnlyTokenMap());
      logger.info("Bootstrapping with streaming options:{}", options);
      RangeStreamer streamer = new RangeStreamer(this.tokenMetadata, this.tokens, this.address, StreamOperation.BOOTSTRAP, useStrictConsistency, DatabaseDescriptor.getEndpointSnitch(), stateStore, true, DatabaseDescriptor.getStreamingConnectionsPerHost(), options.toSourceFilter(DatabaseDescriptor.getEndpointSnitch(), FailureDetector.instance));
      Iterator var5 = Schema.instance.getNonLocalStrategyKeyspaces().iterator();

      while(var5.hasNext()) {
         String keyspaceName = (String)var5.next();
         if(!options.acceptKeyspace(keyspaceName)) {
            logger.warn("Keyspace '{}' is explicitly excluded from bootstrap on user request. Make sure to rebuild the keyspace!");
         } else {
            AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
            streamer.addRanges(keyspaceName, strategy.getPendingAddressRanges(this.tokenMetadata, this.tokens, this.address));
         }
      }

      StreamResultFuture bootstrapStreamResult = streamer.fetchAsync();
      bootstrapStreamResult.addEventListener(new StreamEventHandler() {
         private final AtomicInteger receivedFiles = new AtomicInteger();
         private final AtomicInteger totalFilesToReceive = new AtomicInteger();

         public void handleStreamEvent(StreamEvent event) {
            ProgressEvent currentProgress;
            switch(null.$SwitchMap$org$apache$cassandra$streaming$StreamEvent$Type[event.eventType.ordinal()]) {
            case 1:
               StreamEvent.SessionPreparedEvent prepared = (StreamEvent.SessionPreparedEvent)event;
               int currentTotal = this.totalFilesToReceive.addAndGet((int)prepared.session.getTotalFilesToReceive());
               ProgressEvent prepareProgress = new ProgressEvent(ProgressEventType.PROGRESS, this.receivedFiles.get(), currentTotal, "prepare with " + prepared.session.peer + " complete");
               BootStrapper.this.fireProgressEvent("bootstrap", prepareProgress);
               break;
            case 2:
               StreamEvent.ProgressEvent progress = (StreamEvent.ProgressEvent)event;
               if(progress.progress.isCompleted()) {
                  int received = this.receivedFiles.incrementAndGet();
                  currentProgress = new ProgressEvent(ProgressEventType.PROGRESS, received, this.totalFilesToReceive.get(), "received file " + progress.progress.fileName);
                  BootStrapper.this.fireProgressEvent("bootstrap", currentProgress);
               }
               break;
            case 3:
               StreamEvent.SessionCompleteEvent completeEvent = (StreamEvent.SessionCompleteEvent)event;
               currentProgress = new ProgressEvent(ProgressEventType.PROGRESS, this.receivedFiles.get(), this.totalFilesToReceive.get(), "session with " + completeEvent.peer + " complete");
               BootStrapper.this.fireProgressEvent("bootstrap", currentProgress);
            }

         }

         public void onSuccess(StreamState streamState) {
            ProgressEventType type;
            String message;
            if(streamState.hasFailedSession()) {
               type = ProgressEventType.ERROR;
               message = "Some bootstrap stream failed";
            } else if(streamState.hasAbortedSession()) {
               type = ProgressEventType.ERROR;
               message = "Some bootstrap stream failed";
            } else {
               type = ProgressEventType.SUCCESS;
               message = "Bootstrap streaming success";
            }

            ProgressEvent currentProgress = new ProgressEvent(type, this.receivedFiles.get(), this.totalFilesToReceive.get(), message);
            BootStrapper.this.fireProgressEvent("bootstrap", currentProgress);
         }

         public void onFailure(Throwable throwable) {
            ProgressEvent currentProgress = new ProgressEvent(ProgressEventType.ERROR, this.receivedFiles.get(), this.totalFilesToReceive.get(), throwable.getMessage());
            BootStrapper.this.fireProgressEvent("bootstrap", currentProgress);
         }
      });
      return bootstrapStreamResult;
   }

   public static Collection<Token> getBootstrapTokens(TokenMetadata metadata, InetAddress address, int schemaWaitDelay) throws ConfigurationException {
      String allocationKeyspace = DatabaseDescriptor.getAllocateTokensForKeyspace();
      Integer allocationReplicas = DatabaseDescriptor.getAllocateTokensForLocalReplicationFactor();
      Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
      if(initialTokens.size() > 0 && (allocationKeyspace != null || allocationReplicas != null)) {
         logger.warn("manually specified tokens override automatic allocation");
      }

      if(initialTokens.size() > 0) {
         return getSpecifiedTokens(metadata, initialTokens);
      } else {
         int numTokens = DatabaseDescriptor.getNumTokens();
         if(numTokens < 1) {
            throw new ConfigurationException("num_tokens must be >= 1");
         } else if(allocationKeyspace == null && allocationReplicas == null) {
            if(numTokens == 1) {
               logger.warn("Picking random token for a single vnode.  You should probably add more vnodes and/or use the automatic token allocation mechanism.");
            }

            return getRandomTokens(metadata, numTokens);
         } else {
            return allocateTokens(metadata, address, allocationKeyspace, allocationReplicas, numTokens, schemaWaitDelay);
         }
      }
   }

   private static Collection<Token> getSpecifiedTokens(TokenMetadata metadata, Collection<String> initialTokens) {
      logger.info("tokens manually specified as {}", initialTokens);
      List<Token> tokens = new ArrayList(initialTokens.size());
      Iterator var3 = initialTokens.iterator();

      while(var3.hasNext()) {
         String tokenString = (String)var3.next();
         Token token = metadata.partitioner.getTokenFactory().fromString(tokenString);
         if(metadata.getEndpoint(token) != null) {
            throw new ConfigurationException("Bootstrapping to existing token " + tokenString + " is not allowed (decommission/removenode the old node first).");
         }

         tokens.add(token);
      }

      return tokens;
   }

   static Collection<Token> allocateTokens(TokenMetadata metadata, InetAddress address, String allocationKeyspace, Integer localReplicationFactor, int numTokens, int schemaWaitDelay) {
      StorageService.instance.waitForSchema(schemaWaitDelay);
      if(allocationKeyspace != null) {
         Keyspace ks = Keyspace.open(allocationKeyspace);
         if(ks == null) {
            throw new ConfigurationException("Problem opening token allocation keyspace " + allocationKeyspace);
         } else {
            AbstractReplicationStrategy rs = ks.getReplicationStrategy();
            return TokenAllocation.allocateTokens(metadata, rs, address, numTokens);
         }
      } else if(localReplicationFactor != null) {
         return TokenAllocation.allocateTokens(metadata, localReplicationFactor.intValue(), DatabaseDescriptor.getEndpointSnitch(), address, numTokens);
      } else {
         throw new IllegalArgumentException();
      }
   }

   public static Collection<Token> getRandomTokens(TokenMetadata metadata, int numTokens) {
      HashSet tokens = new HashSet(numTokens);

      while(tokens.size() < numTokens) {
         Token token = metadata.partitioner.getRandomToken();
         if(metadata.getEndpoint(token) == null) {
            tokens.add(token);
         }
      }

      logger.info("Generated random tokens. tokens are {}", tokens);
      return tokens;
   }
}
