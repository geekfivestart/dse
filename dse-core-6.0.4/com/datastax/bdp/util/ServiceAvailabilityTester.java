package com.datastax.bdp.util;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ServiceAvailabilityTester {
   public ServiceAvailabilityTester() {
   }

   public static void checkSocketsAndAwaitCondition(Supplier<Collection<InetSocketAddress>> addressSupplier, Predicate<Collection<InetSocketAddress>> stopCondition, Consumer<Collection<InetSocketAddress>> onFailure) {
      HashSet runningServices = Sets.newHashSet();

      while(!Thread.currentThread().isInterrupted()) {
         Collection<InetSocketAddress> hosts = (Collection)addressSupplier.get();
         runningServices.clear();
         Iterator var5 = hosts.iterator();

         while(var5.hasNext()) {
            InetSocketAddress host = (InetSocketAddress)var5.next();
            if(isSocketListening(host)) {
               runningServices.add(host);
            }
         }

         if(stopCondition.test(runningServices)) {
            break;
         }

         onFailure.accept(runningServices);
      }

   }

   public static boolean isSocketListening(InetSocketAddress host) {
      Socket socket = null;

      boolean var3;
      try {
         socket = new Socket(host.getAddress(), host.getPort());
         boolean var2 = true;
         return var2;
      } catch (IOException var13) {
         var3 = false;
      } finally {
         try {
            if(socket != null) {
               socket.close();
            }
         } catch (IOException var12) {
            ;
         }

      }

      return var3;
   }
}
