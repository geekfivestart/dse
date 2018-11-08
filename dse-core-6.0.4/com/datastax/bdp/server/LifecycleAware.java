package com.datastax.bdp.server;

public interface LifecycleAware {
   default void preSetup() {
   }

   default void postSetup() {
   }

   default void preStart() {
   }

   default void postStart() {
   }

   default void preStop() {
   }

   default void postStop() {
   }

   default void postStartNativeTransport() {
   }

   default void preStopNativeTransport() {
   }
}
