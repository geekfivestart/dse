package org.apache.cassandra.utils.flow;

import java.util.ArrayList;
import java.util.List;

public interface GroupOp<I, O> {
   boolean inSameGroup(I var1, I var2) throws Exception;

   O map(List<I> var1) throws Exception;

   static default <I, O> Flow<O> group(Flow<I> source, GroupOp<I, O> op) {
      return new GroupOp.GroupFlow(op, source);
   }

   public static class GroupFlow<I, O> extends FlowTransform<I, O> {
      final GroupOp<I, O> mapper;
      volatile boolean completeOnNextRequest = false;
      I first;
      List<I> entries;

      public GroupFlow(GroupOp<I, O> mapper, Flow<I> source) {
         super(source);
         this.mapper = mapper;
      }

      public void onFinal(I entry) {
         Object out = null;

         try {
            if(this.first == null || !this.mapper.inSameGroup(this.first, entry)) {
               if(this.first != null) {
                  out = this.mapper.map(this.entries);
               }

               this.entries = new ArrayList();
               this.first = entry;
            }
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         this.entries.add(entry);
         if(out != null) {
            this.completeOnNextRequest = true;
            this.subscriber.onNext(out);
         } else {
            this.onComplete();
         }

      }

      public void onNext(I entry) {
         Object out = null;

         try {
            if(this.first == null || !this.mapper.inSameGroup(this.first, entry)) {
               if(this.first != null) {
                  out = this.mapper.map(this.entries);
               }

               this.entries = new ArrayList();
               this.first = entry;
            }
         } catch (Throwable var4) {
            this.subscriber.onError(var4);
            return;
         }

         this.entries.add(entry);
         if(out != null) {
            this.subscriber.onNext(out);
         } else {
            this.requestInLoop(this.source);
         }

      }

      public void onComplete() {
         Object out = null;

         try {
            if(this.first != null) {
               out = this.mapper.map(this.entries);
               this.first = null;
               this.entries = null;
            }
         } catch (Throwable var3) {
            this.subscriber.onError(var3);
            return;
         }

         if(out != null) {
            this.subscriber.onFinal(out);
         } else {
            this.subscriber.onComplete();
         }

      }

      public void requestNext() {
         if(!this.completeOnNextRequest) {
            this.source.requestNext();
         } else {
            this.onComplete();
         }

      }

      public String toString() {
         return Flow.formatTrace("group", this.mapper, this.sourceFlow);
      }
   }
}
