package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

public class NodeSyncRecord {
   private static final Comparator<NodeSyncRecord> byRangeLeft = Comparator.comparing((r) -> {
      return (Token)r.segment.range.left;
   });
   final Segment segment;
   @Nullable
   final ValidationInfo lastValidation;
   @Nullable
   final ValidationInfo lastSuccessfulValidation;
   @Nullable
   final InetAddress lockedBy;

   public NodeSyncRecord(Segment segment, ValidationInfo lastValidation, ValidationInfo lastSuccessfulValidation, InetAddress lockedBy) {
      assert segment != null;

      assert lastSuccessfulValidation == null || lastValidation != null && lastSuccessfulValidation.outcome.wasSuccessful() && lastSuccessfulValidation.startedAt <= lastValidation.startedAt;

      this.segment = segment;
      this.lastValidation = lastValidation;
      this.lastSuccessfulValidation = lastSuccessfulValidation;
      this.lockedBy = lockedBy;
   }

   private NodeSyncRecord(Segment segment, ValidationInfo lastValidation) {
      this(segment, lastValidation, lastValidation.wasSuccessful()?lastValidation:null, (InetAddress)null);
   }

   @VisibleForTesting
   static NodeSyncRecord empty(Segment segment) {
      return new NodeSyncRecord(segment, (ValidationInfo)null, (ValidationInfo)null, (InetAddress)null);
   }

   long lastValidationTimeMs() {
      return this.lastValidation == null?-9223372036854775808L:this.lastValidation.startedAt;
   }

   long lastSuccessfulValidationTimeMs() {
      return this.lastSuccessfulValidation == null?-9223372036854775808L:this.lastSuccessfulValidation.startedAt;
   }

   boolean isLocked() {
      return this.lockedBy != null;
   }

   static NodeSyncRecord consolidate(Segment segment, List<NodeSyncRecord> coveringRecords) {
      if(coveringRecords.isEmpty()) {
         return empty(segment);
      } else {
         Range<Token> range = segment.range;
         if(coveringRecords.size() == 1) {
            NodeSyncRecord record = (NodeSyncRecord)coveringRecords.get(0);

            assert record.segment.table.equals(segment.table);

            return record.segment.equals(segment)?record:(record.segment.range.contains((AbstractBounds)range)?new NodeSyncRecord(segment, record.lastValidation, record.lastSuccessfulValidation, record.lockedBy):empty(segment));
         } else {
            ValidationInfo lastSuccessfulValidation = consolidateValidations(range, Iterables.filter(coveringRecords, (r) -> {
               return r != null && r.lastSuccessfulValidation != null;
            }), coveringRecords.size(), (r) -> {
               return r.lastSuccessfulValidation;
            });
            ValidationInfo lastValidation = consolidateValidations(range, Iterables.filter(coveringRecords, (r) -> {
               return r != null && r.lastValidation != null;
            }), coveringRecords.size(), (r) -> {
               return r.lastValidation;
            });
            InetAddress lockedBy = consolidateLockedBy(range, coveringRecords);
            return new NodeSyncRecord(segment, lastValidation, lastSuccessfulValidation, lockedBy);
         }
      }
   }

   @VisibleForTesting
   static ValidationInfo consolidateValidations(Range<Token> range, Iterable<NodeSyncRecord> coveringRecords, int maxRecords, Function<NodeSyncRecord, ValidationInfo> getter) {
      PriorityQueue<NodeSyncRecord> records = new PriorityQueue(maxRecords, byRangeLeft);
      Iterables.addAll(records, coveringRecords);

      while(!records.isEmpty() && compareLeftRight((Token)range.left, (Token)((NodeSyncRecord)records.peek()).segment.range.right) >= 0) {
         records.poll();
      }

      if(records.isEmpty()) {
         return null;
      } else {
         List<ValidationInfo> step1Output = new ArrayList();
         NodeSyncRecord first = (NodeSyncRecord)records.poll();
         if(compareLeftLeft((Token)first.segment.range.left, (Token)range.left) > 0) {
            return null;
         } else {
            TableMetadata table = first.segment.table;
            Token currLeft = (Token)first.segment.range.left;
            Token currRight = (Token)first.segment.range.right;
            ValidationInfo currInfo = (ValidationInfo)getter.apply(first);

            while(!records.isEmpty() && compareLeftRight(currLeft, (Token)range.right) < 0) {
               NodeSyncRecord nextRecord = (NodeSyncRecord)records.poll();
               Token nextLeft = (Token)nextRecord.segment.range.left;
               Token nextRight = (Token)nextRecord.segment.range.right;
               ValidationInfo nextInfo = (ValidationInfo)getter.apply(nextRecord);
               int leftRightCmp = compareLeftRight(nextLeft, currRight);
               if(leftRightCmp > 0) {
                  return null;
               }

               if(leftRightCmp == 0) {
                  step1Output.add(currInfo);
                  currInfo = nextInfo;
                  currLeft = nextLeft;
                  currRight = nextRight;
               } else {
                  if(compareLeftLeft(currLeft, nextLeft) < 0) {
                     step1Output.add(currInfo);
                  }

                  int rightRightCmp = compareRightRight(nextRight, currRight);
                  if(rightRightCmp < 0) {
                     records.add(new NodeSyncRecord(new Segment(table, new Range(nextRight, currRight)), currInfo));
                  } else if(rightRightCmp > 0) {
                     records.add(new NodeSyncRecord(new Segment(table, new Range(currRight, nextRight)), nextInfo));
                  }

                  if(nextInfo.startedAt > currInfo.startedAt) {
                     currInfo = nextInfo;
                  }

                  currLeft = nextLeft;
                  currRight = rightRightCmp < 0?nextRight:currRight;
               }
            }

            if(compareRightRight(currRight, (Token)range.right) < 0) {
               return null;
            } else {
               if(compareLeftRight(currLeft, (Token)range.right) < 0) {
                  step1Output.add(currInfo);
               }

               Optional<ValidationInfo> reduced = step1Output.stream().reduce(ValidationInfo::composeWith);

               assert reduced.isPresent() : "The output of step1 shouldn't have been empty, got " + step1Output;

               return (ValidationInfo)reduced.get();
            }
         }
      }
   }

   @VisibleForTesting
   static InetAddress consolidateLockedBy(Range<Token> range, List<NodeSyncRecord> coveringRecords) {
      int i = 0;

      while(compareLeftRight((Token)range.left, (Token)((NodeSyncRecord)coveringRecords.get(i)).segment.range.right) >= 0) {
         ++i;
         if(i == coveringRecords.size()) {
            return null;
         }
      }

      if(compareLeftLeft((Token)((NodeSyncRecord)coveringRecords.get(i)).segment.range.left, (Token)range.left) > 0) {
         return null;
      } else {
         InetAddress lockedBy = ((NodeSyncRecord)coveringRecords.get(i)).lockedBy;
         Token lockedUpTo = (Token)((NodeSyncRecord)coveringRecords.get(i)).segment.range.right;

         while(true) {
            ++i;
            if(i >= coveringRecords.size() || compareRightRight(lockedUpTo, (Token)range.right) >= 0) {
               return compareRightRight(lockedUpTo, (Token)range.right) < 0?null:lockedBy;
            }

            NodeSyncRecord record = (NodeSyncRecord)coveringRecords.get(i);
            if(compareLeftRight((Token)record.segment.range.left, lockedUpTo) > 0) {
               return null;
            }

            if(compareRightRight((Token)record.segment.range.right, lockedUpTo) > 0 && record.lockedBy != null) {
               lockedBy = record.lockedBy;
               lockedUpTo = (Token)record.segment.range.right;
            }
         }
      }
   }

   private static int compareLeftLeft(Token l1, Token l2) {
      return l1.compareTo(l2);
   }

   private static int compareLeftRight(Token l, Token r) {
      return r.isMinimum()?-1:l.compareTo(r);
   }

   private static int compareRightRight(Token r1, Token r2) {
      return r1.isMinimum() && r2.isMinimum()?0:(r1.isMinimum()?1:(r2.isMinimum()?-1:r1.compareTo(r2)));
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{this.segment, this.lastValidation, this.lastSuccessfulValidation, this.lockedBy});
   }

   public boolean equals(Object o) {
      if(!(o instanceof NodeSyncRecord)) {
         return false;
      } else {
         NodeSyncRecord that = (NodeSyncRecord)o;
         return this.segment.equals(that.segment) && Objects.equals(this.lastValidation, that.lastValidation) && Objects.equals(this.lastSuccessfulValidation, that.lastSuccessfulValidation) && Objects.equals(this.lockedBy, that.lockedBy);
      }
   }

   public String toString() {
      String lastSuccString = this.lastSuccessfulValidation != null && (this.lastValidation == null || !this.lastValidation.outcome.wasSuccessful())?", last success=" + this.lastSuccessfulValidation:"";
      String lockStr = this.lockedBy == null?"":", locked by " + this.lockedBy;
      return String.format("%s (last validation=%s%s%s)", new Object[]{this.segment, this.lastValidation == null?"<none>":this.lastValidation.toString(), lastSuccString, lockStr});
   }
}
