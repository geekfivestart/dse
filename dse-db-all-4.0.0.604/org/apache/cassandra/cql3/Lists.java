package org.apache.cassandra.cql3;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public abstract class Lists {
   private Lists() {
   }

   public static ColumnSpecification indexSpecOf(ColumnSpecification column) {
      return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("idx(" + column.name + ")", true), Int32Type.instance);
   }

   public static ColumnSpecification valueSpecOf(ColumnSpecification column) {
      return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((ListType)column.type).getElementsType());
   }

   public static AssignmentTestable.TestResult testListAssignment(ColumnSpecification receiver, List<? extends AssignmentTestable> elements) {
      if(!(receiver.type instanceof ListType)) {
         return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
      } else if(elements.isEmpty()) {
         return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      } else {
         ColumnSpecification valueSpec = valueSpecOf(receiver);
         return AssignmentTestable.TestResult.testAll(receiver.ksName, valueSpec, elements);
      }
   }

   public static String listToString(List<?> elements) {
      return listToString(elements, Object::toString);
   }

   public static <T> String listToString(Iterable<T> items, Function<T, String> mapper) {
      return (String)StreamSupport.stream(items.spliterator(), false).map((e) -> {
         return (String)mapper.apply(e);
      }).collect(Collectors.joining(", ", "[", "]"));
   }

   public static <T> AbstractType<?> getExactListTypeIfKnown(List<T> items, Function<T, AbstractType<?>> mapper) {
      AbstractType<?> type = getElementType(items, mapper);
      return type != null?ListType.getInstance(type, false):null;
   }

   protected static <T> AbstractType<?> getElementType(List<T> items, Function<T, AbstractType<?>> mapper) {
      AbstractType<?> type = null;
      Iterator var3 = items.iterator();

      AbstractType itemType;
      do {
         while(true) {
            do {
               if(!var3.hasNext()) {
                  return type;
               }

               T item = var3.next();
               itemType = (AbstractType)mapper.apply(item);
            } while(itemType == null);

            if(type != null && !itemType.isCompatibleWith(type)) {
               break;
            }

            type = itemType;
         }
      } while(type.isCompatibleWith(itemType));

      throw new InvalidRequestException("Invalid collection literal: all selectors must have the same CQL type inside collection literals");
   }

   private static int existingSize(Row row, ColumnMetadata column) {
      if(row == null) {
         return 0;
      } else {
         ComplexColumnData complexData = row.getComplexColumnData(column);
         return complexData == null?0:complexData.cellsCount();
      }
   }

   public static class DiscarderByIndex extends Operation {
      public DiscarderByIndex(ColumnMetadata column, Term idx) {
         super(column, idx);
      }

      public boolean requiresRead() {
         return true;
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to delete an item by index from a frozen list";

         Term.Terminal index = this.t.bind(params.options);
         if(index == null) {
            throw new InvalidRequestException("Invalid null value for list index");
         } else if(index != Constants.UNSET_VALUE) {
            Row existingRow = params.getPrefetchedRow(partitionKey, params.currentClustering());
            int existingSize = Lists.existingSize(existingRow, this.column);
            int idx = ByteBufferUtil.toInt(index.get(params.options.getProtocolVersion()));
            if(existingSize == 0) {
               throw new InvalidRequestException("Attempted to delete an element from a list which is null");
            } else if(idx >= 0 && idx < existingSize) {
               params.addTombstone(this.column, existingRow.getComplexColumnData(this.column).getCellByIndex(idx).path());
            } else {
               throw new InvalidRequestException(String.format("List index %d out of bound, list has size %d", new Object[]{Integer.valueOf(idx), Integer.valueOf(existingSize)}));
            }
         }
      }
   }

   public static class Discarder extends Operation {
      public Discarder(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public boolean requiresRead() {
         return true;
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to delete from a frozen list";

         Term.Terminal value = this.t.bind(params.options);
         Row existingRow = params.getPrefetchedRow(partitionKey, params.currentClustering());
         ComplexColumnData complexData = existingRow == null?null:existingRow.getComplexColumnData(this.column);
         if(value != null && value != Constants.UNSET_VALUE && complexData != null) {
            List<ByteBuffer> toDiscard = ((Lists.Value)value).elements;
            Iterator var7 = complexData.iterator();

            while(var7.hasNext()) {
               Cell cell = (Cell)var7.next();
               if(toDiscard.contains(cell.value())) {
                  params.addTombstone(this.column, cell.path());
               }
            }

         }
      }
   }

   public static class Prepender extends Operation {
      public Prepender(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to prepend to a frozen list";

         Term.Terminal value = this.t.bind(params.options);
         if(value != null && value != Constants.UNSET_VALUE) {
            List<ByteBuffer> toAdd = ((Lists.Value)value).elements;
            int totalCount = toAdd.size();
            Lists.PrecisionTime pt = null;
            int remainingInBatch = 0;

            for(int i = totalCount - 1; i >= 0; --i) {
               if(remainingInBatch == 0) {
                  long time = 1262304000000L - (System.currentTimeMillis() - 1262304000000L);
                  remainingInBatch = Math.min(9999, i) + 1;
                  pt = Lists.PrecisionTime.getNext(time, remainingInBatch);
               }

               ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(pt.millis, pt.nanos + remainingInBatch--));
               params.addCell(this.column, CellPath.create(uuid), (ByteBuffer)toAdd.get(i));
            }

         }
      }
   }

   public static class Appender extends Operation {
      public Appender(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to append to a frozen list";

         Term.Terminal value = this.t.bind(params.options);
         doAppend(value, this.column, params);
      }

      static void doAppend(Term.Terminal value, ColumnMetadata column, UpdateParameters params) throws InvalidRequestException {
         if(column.type.isMultiCell()) {
            if(value == null) {
               return;
            }

            Iterator var3 = ((Lists.Value)value).elements.iterator();

            while(var3.hasNext()) {
               ByteBuffer buffer = (ByteBuffer)var3.next();
               ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
               params.addCell(column, CellPath.create(uuid), buffer);
            }
         } else if(value == null) {
            params.addTombstone(column);
         } else {
            params.addCell(column, value.get(ProtocolVersion.CURRENT));
         }

      }
   }

   public static class SetterByIndex extends Operation {
      private final Term idx;

      public SetterByIndex(ColumnMetadata column, Term idx, Term t) {
         super(column, t);
         this.idx = idx;
      }

      public boolean requiresRead() {
         return true;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
         super.collectMarkerSpecification(boundNames);
         this.idx.collectMarkerSpecification(boundNames);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to set an individual element on a frozen list";

         ByteBuffer index = this.idx.bindAndGet(params.options);
         ByteBuffer value = this.t.bindAndGet(params.options);
         if(index == null) {
            throw new InvalidRequestException("Invalid null value for list index");
         } else if(index == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            throw new InvalidRequestException("Invalid unset value for list index");
         } else {
            Row existingRow = params.getPrefetchedRow(partitionKey, params.currentClustering());
            int existingSize = Lists.existingSize(existingRow, this.column);
            int idx = ByteBufferUtil.toInt(index);
            if(existingSize == 0) {
               throw new InvalidRequestException("Attempted to set an element on a list which is null");
            } else if(idx >= 0 && idx < existingSize) {
               CellPath elementPath = existingRow.getComplexColumnData(this.column).getCellByIndex(idx).path();
               if(value == null) {
                  params.addTombstone(this.column, elementPath);
               } else if(value != ByteBufferUtil.UNSET_BYTE_BUFFER) {
                  params.addCell(this.column, elementPath, value);
               }

            } else {
               throw new InvalidRequestException(String.format("List index %d out of bound, list has size %d", new Object[]{Integer.valueOf(idx), Integer.valueOf(existingSize)}));
            }
         }
      }
   }

   public static class Setter extends Operation {
      public Setter(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         Term.Terminal value = this.t.bind(params.options);
         if(value != Constants.UNSET_VALUE) {
            if(this.column.type.isMultiCell()) {
               params.setComplexDeletionTimeForOverwrite(this.column);
            }

            Lists.Appender.doAppend(value, this.column, params);
         }
      }
   }

   static class PrecisionTime {
      private static final long REFERENCE_TIME = 1262304000000L;
      static final int MAX_NANOS = 9999;
      private static final AtomicReference<Lists.PrecisionTime> last = new AtomicReference(new Lists.PrecisionTime(9223372036854775807L, 0));
      public final long millis;
      public final int nanos;

      PrecisionTime(long millis, int nanos) {
         this.millis = millis;
         this.nanos = nanos;
      }

      static Lists.PrecisionTime getNext(long millis, int count) {
         if(count == 0) {
            return (Lists.PrecisionTime)last.get();
         } else {
            Lists.PrecisionTime current;
            Lists.PrecisionTime next;
            do {
               current = (Lists.PrecisionTime)last.get();
               if(millis < current.millis) {
                  next = new Lists.PrecisionTime(millis, 9999 - count);
               } else {
                  long millisToUse = millis <= current.millis?millis:current.millis;
                  int nanosToUse;
                  if(current.nanos - count >= 0) {
                     nanosToUse = current.nanos - count;
                  } else {
                     nanosToUse = 9999 - count;
                     --millisToUse;
                  }

                  next = new Lists.PrecisionTime(millisToUse, nanosToUse);
               }
            } while(!last.compareAndSet(current, next));

            return next;
         }
      }

      @VisibleForTesting
      static void set(long millis, int nanos) {
         last.set(new Lists.PrecisionTime(millis, nanos));
      }
   }

   public static class Marker extends AbstractMarker {
      protected Marker(int bindIndex, ColumnSpecification receiver) {
         super(bindIndex, receiver);

         assert receiver.type instanceof ListType;

      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         ByteBuffer value = (ByteBuffer)options.getValues().get(this.bindIndex);
         return (Term.Terminal)(value == null?null:(value == ByteBufferUtil.UNSET_BYTE_BUFFER?Constants.UNSET_VALUE:Lists.Value.fromSerialized(value, (ListType)this.receiver.type, options.getProtocolVersion())));
      }
   }

   public static class DelayedValue extends Term.NonTerminal {
      private final List<Term> elements;

      public DelayedValue(List<Term> elements) {
         this.elements = elements;
      }

      public boolean containsBindMarker() {
         return false;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         List<ByteBuffer> buffers = new ArrayList(this.elements.size());
         Iterator var3 = this.elements.iterator();

         while(var3.hasNext()) {
            Term t = (Term)var3.next();
            ByteBuffer bytes = t.bindAndGet(options);
            if(bytes == null) {
               throw new InvalidRequestException("null is not supported inside collections");
            }

            if(bytes == ByteBufferUtil.UNSET_BYTE_BUFFER) {
               return Constants.UNSET_VALUE;
            }

            buffers.add(bytes);
         }

         return new Lists.Value(buffers);
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
         Terms.addFunctions(this.elements, functions);
      }
   }

   public static class Value extends Term.MultiItemTerminal {
      public final List<ByteBuffer> elements;

      public Value(List<ByteBuffer> elements) {
         this.elements = elements;
      }

      public static Lists.Value fromSerialized(ByteBuffer value, ListType type, ProtocolVersion version) throws InvalidRequestException {
         try {
            List<?> l = type.getSerializer().deserializeForNativeProtocol(value, version);
            List<ByteBuffer> elements = new ArrayList(l.size());
            Iterator var5 = l.iterator();

            while(var5.hasNext()) {
               Object element = var5.next();
               elements.add(element == null?null:type.getElementsType().decompose(element));
            }

            return new Lists.Value(elements);
         } catch (MarshalException var7) {
            throw new InvalidRequestException(var7.getMessage());
         }
      }

      public ByteBuffer get(ProtocolVersion protocolVersion) {
         return CollectionSerializer.pack(this.elements, this.elements.size(), protocolVersion);
      }

      public boolean equals(ListType lt, Lists.Value v) {
         if(this.elements.size() != v.elements.size()) {
            return false;
         } else {
            for(int i = 0; i < this.elements.size(); ++i) {
               if(lt.getElementsType().compare((ByteBuffer)this.elements.get(i), (ByteBuffer)v.elements.get(i)) != 0) {
                  return false;
               }
            }

            return true;
         }
      }

      public List<ByteBuffer> getElements() {
         return this.elements;
      }
   }

   public static class Literal extends Term.Raw {
      private final List<Term.Raw> elements;

      public Literal(List<Term.Raw> elements) {
         this.elements = elements;
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         this.validateAssignableTo(keyspace, receiver);
         ColumnSpecification valueSpec = Lists.valueSpecOf(receiver);
         List<Term> values = new ArrayList(this.elements.size());
         boolean allTerminal = true;

         Term t;
         for(Iterator var6 = this.elements.iterator(); var6.hasNext(); values.add(t)) {
            Term.Raw rt = (Term.Raw)var6.next();
            t = rt.prepare(keyspace, valueSpec);
            if(t.containsBindMarker()) {
               throw new InvalidRequestException(String.format("Invalid list literal for %s: bind variables are not supported inside collection literals", new Object[]{receiver.name}));
            }

            if(t instanceof Term.NonTerminal) {
               allTerminal = false;
            }
         }

         Lists.DelayedValue value = new Lists.DelayedValue(values);
         return (Term)(allTerminal?value.bind(QueryOptions.DEFAULT):value);
      }

      private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if(!(receiver.type instanceof ListType)) {
            throw new InvalidRequestException(String.format("Invalid list literal for %s of type %s", new Object[]{receiver.name, receiver.type.asCQL3Type()}));
         } else {
            ColumnSpecification valueSpec = Lists.valueSpecOf(receiver);
            Iterator var4 = this.elements.iterator();

            Term.Raw rt;
            do {
               if(!var4.hasNext()) {
                  return;
               }

               rt = (Term.Raw)var4.next();
            } while(rt.testAssignment(keyspace, valueSpec).isAssignable());

            throw new InvalidRequestException(String.format("Invalid list literal for %s: value %s is not of type %s", new Object[]{receiver.name, rt, valueSpec.type.asCQL3Type()}));
         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return Lists.testListAssignment(receiver, this.elements);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return Lists.getExactListTypeIfKnown(this.elements, (p) -> {
            return p.getExactTypeIfKnown(keyspace);
         });
      }

      public String getText() {
         return Lists.listToString(this.elements, Term.Raw::getText);
      }
   }
}
