package com.datastax.bdp.db.nodesync;

abstract class ValidationProposal {
   protected final TableState.Ref segmentRef;

   ValidationProposal(TableState.Ref segmentRef) {
      this.segmentRef = segmentRef;
   }

   Segment segment() {
      return this.segmentRef.segment();
   }

   abstract Validator activate();
}
