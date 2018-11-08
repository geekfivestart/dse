package com.datastax.bdp.node.transport;

public interface ServerProcessor<I, O> {
   Message<O> process(RequestContext var1, I var2) throws Exception;

   void onComplete(Message<O> var1);
}
