package com.datastax.bdp.gms;

import java.net.InetAddress;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;

public abstract class EndpointStateChangeAdapter implements IEndpointStateChangeSubscriber {
   public EndpointStateChangeAdapter() {
   }

   public void onJoin(InetAddress inetAddress, EndpointState endpointState) {
   }

   public void beforeChange(InetAddress inetAddress, EndpointState endpointState, ApplicationState applicationState, VersionedValue versionedValue) {
   }

   public void onChange(InetAddress inetAddress, ApplicationState applicationState, VersionedValue versionedValue) {
   }

   public void onAlive(InetAddress inetAddress, EndpointState endpointState) {
   }

   public void onDead(InetAddress inetAddress, EndpointState endpointState) {
   }

   public void onRemove(InetAddress inetAddress) {
   }

   public void onRestart(InetAddress inetAddress, EndpointState endpointState) {
   }
}
