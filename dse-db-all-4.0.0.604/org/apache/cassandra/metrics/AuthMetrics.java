package org.apache.cassandra.metrics;

public class AuthMetrics {
   public static final AuthMetrics instance = new AuthMetrics();
   protected final Meter success;
   protected final Meter failure;

   public static void init() {
   }

   private AuthMetrics() {
      this.success = ClientMetrics.instance.addMeter("AuthSuccess");
      this.failure = ClientMetrics.instance.addMeter("AuthFailure");
   }

   public void markSuccess() {
      this.success.mark();
   }

   public void markFailure() {
      this.failure.mark();
   }
}
