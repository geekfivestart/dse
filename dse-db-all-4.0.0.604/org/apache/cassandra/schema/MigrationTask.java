package org.apache.cassandra.schema;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MigrationTask extends WrappedRunnable {
   private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);
   private static final ConcurrentLinkedQueue<CountDownLatch> inflightTasks = new ConcurrentLinkedQueue();
   private static final Set<SystemKeyspace.BootstrapState> monitoringBootstrapStates;
   private final InetAddress endpoint;

   MigrationTask(InetAddress endpoint) {
      this.endpoint = endpoint;
   }

   static ConcurrentLinkedQueue<CountDownLatch> getInflightTasks() {
      return inflightTasks;
   }

   public void runMayThrow() throws Exception {
      if(!FailureDetector.instance.isAlive(this.endpoint)) {
         logger.warn("Can't send schema pull request: node {} is down.", this.endpoint);
      } else {
         final CountDownLatch completionLatch = new CountDownLatch(1);
         MessageCallback<SchemaMigration> cb = new MessageCallback<SchemaMigration>() {
            public void onResponse(Response<SchemaMigration> message) {
               try {
                  SchemaMigration migration = (SchemaMigration)message.payload();
                  if(!migration.isCompatible) {
                     MigrationTask.logger.debug("Tried Schema migration from {}, but it has incompatible schema", MigrationTask.this.endpoint);
                     return;
                  }

                  MigrationTask.logger.debug("Got schema migration response from {}", message.from());
                  Schema.instance.mergeAndAnnounceVersion((SchemaMigration)message.payload());
                  MigrationTask.logger.debug("Merged schema migration response from {}", message.from());
               } catch (ConfigurationException var6) {
                  MigrationTask.logger.error("Configuration exception merging remote schema", var6);
               } finally {
                  completionLatch.countDown();
               }

            }

            public void onFailure(FailureResponse response) {
               MigrationTask.logger.error("Unexpected error during schema migration from {} (reason: {})", MigrationTask.this.endpoint, response.reason());
            }
         };
         if(monitoringBootstrapStates.contains(SystemKeyspace.getBootstrapState())) {
            logger.debug("Will wait at max {} second(s) for migration response from {}", Integer.valueOf(MigrationManager.MIGRATION_TASK_WAIT_IN_SECONDS), this.endpoint);
            inflightTasks.offer(completionLatch);
         }

         logger.debug("Pulling schema from endpoint {}", this.endpoint);
         MessagingService.instance().send(Verbs.SCHEMA.PULL.newRequest(this.endpoint, PullRequest.create()), cb);
      }
   }

   static {
      monitoringBootstrapStates = EnumSet.of(SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP, SystemKeyspace.BootstrapState.IN_PROGRESS);
   }
}
