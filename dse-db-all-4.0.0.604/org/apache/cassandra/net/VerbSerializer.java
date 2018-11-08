package org.apache.cassandra.net;

import org.apache.cassandra.utils.Serializer;

class VerbSerializer<P, Q> {
   final Verb<P, Q> verb;
   final int code;
   final Serializer<P> requestSerializer;
   final Serializer<Q> responseSerializer;

   VerbSerializer(Verb<P, Q> verb, int code, Serializer<P> requestSerializer, Serializer<Q> responseSerializer) {
      this.verb = verb;
      this.code = code;
      this.requestSerializer = requestSerializer;
      this.responseSerializer = responseSerializer;
   }
}
