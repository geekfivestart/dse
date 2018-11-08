package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

public class ExpirationDateOverflowHandling {
   private static final Logger logger = LoggerFactory.getLogger(ExpirationDateOverflowHandling.class);
   private static final int EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES = Integer.getInteger("cassandra.expiration_overflow_warning_interval_minutes", 5).intValue();
   @VisibleForTesting
   public static ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy policy;
   public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING = "Request on table {}.{} with {}ttl of {} seconds exceeds maximum supported expiration date of 2038-01-19T03:14:06+00:00 and will have its expiration capped to that date. In order to avoid this use a lower TTL or upgrade to a version where this limitation is fixed. See CASSANDRA-14092 for more details.";
   public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE = "Request on table %s.%s with %sttl of %d seconds exceeds maximum supported expiration date of 2038-01-19T03:14:06+00:00. In order to avoid this use a lower TTL, change the expiration date overflow policy or upgrade to a version where this limitation is fixed. See CASSANDRA-14092 for more details.";

   public ExpirationDateOverflowHandling() {
   }

   public static void maybeApplyExpirationDateOverflowPolicy(TableMetadata metadata, int ttl, boolean isDefaultTTL) throws InvalidRequestException {
      if(ttl != 0) {
         int nowInSecs = (int)(System.currentTimeMillis() / 1000L);
         if(ttl + nowInSecs < 0) {
            switch(null.$SwitchMap$org$apache$cassandra$db$ExpirationDateOverflowHandling$ExpirationDateOverflowPolicy[policy.ordinal()]) {
            case 1:
               ClientWarn.instance.warn(MessageFormatter.arrayFormat("Request on table {}.{} with {}ttl of {} seconds exceeds maximum supported expiration date of 2038-01-19T03:14:06+00:00 and will have its expiration capped to that date. In order to avoid this use a lower TTL or upgrade to a version where this limitation is fixed. See CASSANDRA-14092 for more details.", new Object[]{metadata.keyspace, metadata.name, isDefaultTTL?"default ":"", Integer.valueOf(ttl)}).getMessage());
            case 2:
               NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, (long)EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES, TimeUnit.MINUTES, "Request on table {}.{} with {}ttl of {} seconds exceeds maximum supported expiration date of 2038-01-19T03:14:06+00:00 and will have its expiration capped to that date. In order to avoid this use a lower TTL or upgrade to a version where this limitation is fixed. See CASSANDRA-14092 for more details.", new Object[]{metadata.keyspace, metadata.name, isDefaultTTL?"default ":"", Integer.valueOf(ttl)});
               return;
            default:
               throw new InvalidRequestException(String.format("Request on table %s.%s with %sttl of %d seconds exceeds maximum supported expiration date of 2038-01-19T03:14:06+00:00. In order to avoid this use a lower TTL, change the expiration date overflow policy or upgrade to a version where this limitation is fixed. See CASSANDRA-14092 for more details.", new Object[]{metadata.keyspace, metadata.name, isDefaultTTL?"default ":"", Integer.valueOf(ttl)}));
            }
         }
      }
   }

   public static int computeLocalExpirationTime(int nowInSec, int timeToLive) {
      int localExpirationTime = nowInSec + timeToLive;
      return localExpirationTime >= 0?localExpirationTime:2147483646;
   }

   static {
      String policyAsString = System.getProperty("cassandra.expiration_date_overflow_policy", ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy.REJECT.name());

      try {
         policy = ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy.valueOf(policyAsString.toUpperCase());
      } catch (RuntimeException var2) {
         logger.warn("Invalid expiration date overflow policy: {}. Using default: {}", policyAsString, ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy.REJECT.name());
         policy = ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy.REJECT;
      }

   }

   public static enum ExpirationDateOverflowPolicy {
      REJECT,
      CAP_NOWARN,
      CAP;

      private ExpirationDateOverflowPolicy() {
      }
   }
}
