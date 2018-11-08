package com.datastax.bdp.util;

import com.google.common.annotations.VisibleForTesting;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class WeakListenerSupport<L> {
   private final CopyOnWriteArrayList<WeakReference<L>> listeners = new CopyOnWriteArrayList();

   public WeakListenerSupport() {
   }

   @VisibleForTesting
   public int getListenerCount() {
      return this.listeners.size();
   }

   public final synchronized void addListener(L listener) {
      this.listeners.removeIf((wr) -> {
         return wr.get() == null;
      });
      if(this.listeners.stream().noneMatch((wr) -> {
         return ((Boolean)Optional.ofNullable(wr.get()).map((l) -> {
            return Boolean.valueOf(Objects.equals(listener, l));
         }).orElse(Boolean.valueOf(false))).booleanValue();
      })) {
         this.listeners.add(new WeakReference(listener));
      }

   }

   public final synchronized void removeListener(L listener) {
      this.listeners.removeIf((wr) -> {
         return ((Boolean)Optional.ofNullable(wr.get()).map((l) -> {
            return Boolean.valueOf(Objects.equals(listener, l));
         }).orElse(Boolean.valueOf(true))).booleanValue();
      });
   }

   public final void notifyListeners(Consumer<L> notificationFunction) {
      Iterator var2 = this.listeners.iterator();

      while(var2.hasNext()) {
         WeakReference<L> wr = (WeakReference)var2.next();
         L listener = wr.get();
         if(listener != null) {
            notificationFunction.accept(listener);
         }
      }

   }
}
