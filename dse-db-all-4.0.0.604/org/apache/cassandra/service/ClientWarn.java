package org.apache.cassandra.service;

import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.ExecutorLocal;

public class ClientWarn implements ExecutorLocal<ClientWarn.State> {
   private static final String TRUNCATED = " [truncated]";
   private static final FastThreadLocal<ClientWarn.State> warnLocal = new FastThreadLocal();
   private static final ConcurrentMap<Integer, ClientWarn.State> statesByMessageId = new ConcurrentHashMap();
   public static ClientWarn instance = new ClientWarn();

   private ClientWarn() {
   }

   public ClientWarn.State get() {
      return (ClientWarn.State)warnLocal.get();
   }

   public void set(ClientWarn.State value) {
      warnLocal.set(value);
   }

   public void warn(String text) {
      ClientWarn.State state = (ClientWarn.State)warnLocal.get();
      if(state != null) {
         state.add(text);
      }

   }

   public void captureWarnings() {
      warnLocal.set(new ClientWarn.State());
   }

   public List<String> getWarnings() {
      ClientWarn.State state = (ClientWarn.State)warnLocal.get();
      return state != null && !state.warnings.isEmpty()?state.warnings:null;
   }

   public void resetWarnings() {
      warnLocal.remove();
   }

   public void storeForRequest(int id) {
      ClientWarn.State state = (ClientWarn.State)warnLocal.get();
      if(state != null) {
         statesByMessageId.put(Integer.valueOf(id), state);
      }

   }

   @Nullable
   public ClientWarn.State getForMessage(int id) {
      return (ClientWarn.State)statesByMessageId.remove(Integer.valueOf(id));
   }

   public static class State {
      private final List<String> warnings = new ArrayList();

      public State() {
      }

      private void add(String warning) {
         if(this.warnings.size() < '\uffff') {
            this.warnings.add(maybeTruncate(warning));
         }

      }

      private static String maybeTruncate(String warning) {
         return warning.length() > '\uffff'?warning.substring(0, '\uffff' - " [truncated]".length()) + " [truncated]":warning;
      }
   }
}
