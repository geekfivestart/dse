package org.apache.cassandra.cql3.conditions;

import com.google.common.collect.Iterators;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class ColumnCondition {
   public final ColumnMetadata column;
   public final Operator operator;
   private final Terms terms;

   private ColumnCondition(ColumnMetadata column, Operator op, Terms terms) {
      this.column = column;
      this.operator = op;
      this.terms = terms;
   }

   public void addFunctionsTo(List<Function> functions) {
      this.terms.addFunctionsTo(functions);
   }

   public void collectMarkerSpecification(VariableSpecifications boundNames) {
      this.terms.collectMarkerSpecification(boundNames);
   }

   public abstract ColumnCondition.Bound bind(QueryOptions var1);

   protected final List<ByteBuffer> bindAndGetTerms(QueryOptions options) {
      return this.filterUnsetValuesIfNeeded(this.checkValues(this.terms.bindAndGet(options)));
   }

   protected final List<Term.Terminal> bindTerms(QueryOptions options) {
      return this.filterUnsetValuesIfNeeded(this.checkValues(this.terms.bind(options)));
   }

   private <T> List<T> checkValues(List<T> values) {
      RequestValidations.checkFalse(values == null && this.operator.isIN(), "Invalid null list in IN condition");
      RequestValidations.checkFalse(values == Terms.UNSET_LIST, "Invalid 'unset' value in condition");
      return values;
   }

   private <T> List<T> filterUnsetValuesIfNeeded(List<T> values) {
      if(!this.operator.isIN()) {
         return values;
      } else {
         List<T> filtered = new ArrayList(values.size());
         int i = 0;

         for(int m = values.size(); i < m; ++i) {
            T value = values.get(i);
            if(value != ByteBufferUtil.UNSET_BYTE_BUFFER && value != Constants.UNSET_VALUE) {
               filtered.add(value);
            }
         }

         return filtered;
      }
   }

   public static ColumnCondition condition(ColumnMetadata column, Operator op, Terms terms) {
      return new ColumnCondition.SimpleColumnCondition(column, op, terms);
   }

   public static ColumnCondition condition(ColumnMetadata column, Term collectionElement, Operator op, Terms terms) {
      return new ColumnCondition.CollectionElementCondition(column, collectionElement, op, terms);
   }

   public static ColumnCondition condition(ColumnMetadata column, FieldIdentifier udtField, Operator op, Terms terms) {
      return new ColumnCondition.UDTFieldCondition(column, udtField, op, terms);
   }

   protected static final Cell getCell(Row row, ColumnMetadata column) {
      return row == null?null:row.getCell(column);
   }

   protected static final Cell getCell(Row row, ColumnMetadata column, CellPath path) {
      return row == null?null:row.getCell(column, path);
   }

   protected static final Iterator<Cell> getCells(Row row, ColumnMetadata column) {
      if(row == null) {
         return Collections.emptyIterator();
      } else {
         ComplexColumnData complexData = row.getComplexColumnData(column);
         return complexData == null?Collections.emptyIterator():complexData.iterator();
      }
   }

   protected static final boolean evaluateComparisonWithOperator(int comparison, Operator operator) {
      switch(null.$SwitchMap$org$apache$cassandra$cql3$Operator[operator.ordinal()]) {
      case 1:
         return false;
      case 2:
         return true;
      case 3:
      case 4:
         return comparison < 0;
      case 5:
      case 6:
         return comparison > 0;
      default:
         throw new AssertionError();
      }
   }

   public static class Raw {
      private final Term.Raw value;
      private final List<Term.Raw> inValues;
      private final AbstractMarker.INRaw inMarker;
      private final Term.Raw collectionElement;
      private final FieldIdentifier udtField;
      private final Operator operator;

      private Raw(Term.Raw value, List<Term.Raw> inValues, AbstractMarker.INRaw inMarker, Term.Raw collectionElement, FieldIdentifier udtField, Operator op) {
         this.value = value;
         this.inValues = inValues;
         this.inMarker = inMarker;
         this.collectionElement = collectionElement;
         this.udtField = udtField;
         this.operator = op;
      }

      public static ColumnCondition.Raw simpleCondition(Term.Raw value, Operator op) {
         return new ColumnCondition.Raw(value, (List)null, (AbstractMarker.INRaw)null, (Term.Raw)null, (FieldIdentifier)null, op);
      }

      public static ColumnCondition.Raw simpleInCondition(List<Term.Raw> inValues) {
         return new ColumnCondition.Raw((Term.Raw)null, inValues, (AbstractMarker.INRaw)null, (Term.Raw)null, (FieldIdentifier)null, Operator.IN);
      }

      public static ColumnCondition.Raw simpleInCondition(AbstractMarker.INRaw inMarker) {
         return new ColumnCondition.Raw((Term.Raw)null, (List)null, inMarker, (Term.Raw)null, (FieldIdentifier)null, Operator.IN);
      }

      public static ColumnCondition.Raw collectionCondition(Term.Raw value, Term.Raw collectionElement, Operator op) {
         return new ColumnCondition.Raw(value, (List)null, (AbstractMarker.INRaw)null, collectionElement, (FieldIdentifier)null, op);
      }

      public static ColumnCondition.Raw collectionInCondition(Term.Raw collectionElement, List<Term.Raw> inValues) {
         return new ColumnCondition.Raw((Term.Raw)null, inValues, (AbstractMarker.INRaw)null, collectionElement, (FieldIdentifier)null, Operator.IN);
      }

      public static ColumnCondition.Raw collectionInCondition(Term.Raw collectionElement, AbstractMarker.INRaw inMarker) {
         return new ColumnCondition.Raw((Term.Raw)null, (List)null, inMarker, collectionElement, (FieldIdentifier)null, Operator.IN);
      }

      public static ColumnCondition.Raw udtFieldCondition(Term.Raw value, FieldIdentifier udtField, Operator op) {
         return new ColumnCondition.Raw(value, (List)null, (AbstractMarker.INRaw)null, (Term.Raw)null, udtField, op);
      }

      public static ColumnCondition.Raw udtFieldInCondition(FieldIdentifier udtField, List<Term.Raw> inValues) {
         return new ColumnCondition.Raw((Term.Raw)null, inValues, (AbstractMarker.INRaw)null, (Term.Raw)null, udtField, Operator.IN);
      }

      public static ColumnCondition.Raw udtFieldInCondition(FieldIdentifier udtField, AbstractMarker.INRaw inMarker) {
         return new ColumnCondition.Raw((Term.Raw)null, (List)null, inMarker, (Term.Raw)null, udtField, Operator.IN);
      }

      public ColumnCondition prepare(String keyspace, ColumnMetadata receiver, TableMetadata cfm) {
         if(receiver.type instanceof CounterColumnType) {
            throw RequestValidations.invalidRequest("Conditions on counters are not supported");
         } else if(this.collectionElement != null) {
            if(!receiver.type.isCollection()) {
               throw RequestValidations.invalidRequest("Invalid element access syntax for non-collection column %s", new Object[]{receiver.name});
            } else {
               ColumnSpecification elementSpec;
               ColumnSpecification valueSpec;
               switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)receiver.type).kind.ordinal()]) {
               case 1:
                  elementSpec = Lists.indexSpecOf(receiver);
                  valueSpec = Lists.valueSpecOf(receiver);
                  break;
               case 2:
                  throw RequestValidations.invalidRequest("Invalid element access syntax for set column %s", new Object[]{receiver.name});
               case 3:
                  elementSpec = Maps.keySpecOf(receiver);
                  valueSpec = Maps.valueSpecOf(receiver);
                  break;
               default:
                  throw new AssertionError();
               }

               this.validateOperationOnDurations(valueSpec.type);
               return ColumnCondition.condition(receiver, this.collectionElement.prepare(keyspace, elementSpec), this.operator, this.prepareTerms(keyspace, valueSpec));
            }
         } else if(this.udtField != null) {
            UserType userType = (UserType)receiver.type;
            int fieldPosition = userType.fieldPosition(this.udtField);
            if(fieldPosition == -1) {
               throw RequestValidations.invalidRequest("Unknown field %s for column %s", new Object[]{this.udtField, receiver.name});
            } else {
               ColumnSpecification fieldReceiver = UserTypes.fieldSpecOf(receiver, fieldPosition);
               this.validateOperationOnDurations(fieldReceiver.type);
               return ColumnCondition.condition(receiver, this.udtField, this.operator, this.prepareTerms(keyspace, fieldReceiver));
            }
         } else {
            this.validateOperationOnDurations(receiver.type);
            return ColumnCondition.condition(receiver, this.operator, this.prepareTerms(keyspace, receiver));
         }
      }

      private Terms prepareTerms(String keyspace, ColumnSpecification receiver) {
         return this.operator.isIN()?(this.inValues == null?Terms.ofListMarker(this.inMarker.prepare(keyspace, receiver), receiver.type):Terms.of(prepareTerms(keyspace, receiver, this.inValues))):Terms.of(this.value.prepare(keyspace, receiver));
      }

      private static List<Term> prepareTerms(String keyspace, ColumnSpecification receiver, List<Term.Raw> raws) {
         List<Term> terms = new ArrayList(raws.size());
         int i = 0;

         for(int m = raws.size(); i < m; ++i) {
            Term.Raw raw = (Term.Raw)raws.get(i);
            terms.add(raw.prepare(keyspace, receiver));
         }

         return terms;
      }

      private void validateOperationOnDurations(AbstractType<?> type) {
         if(type.referencesDuration() && this.operator.isSlice()) {
            RequestValidations.checkFalse(type.isCollection(), "Slice conditions are not supported on collections containing durations");
            RequestValidations.checkFalse(type.isTuple(), "Slice conditions are not supported on tuples containing durations");
            RequestValidations.checkFalse(type.isUDT(), "Slice conditions are not supported on UDTs containing durations");
            throw RequestValidations.invalidRequest("Slice conditions ( %s ) are not supported on durations", new Object[]{this.operator});
         }
      }
   }

   private static final class MultiCellUdtBound extends ColumnCondition.Bound {
      private final List<ByteBuffer> values;
      private final ProtocolVersion protocolVersion;

      private MultiCellUdtBound(ColumnMetadata column, Operator op, List<ByteBuffer> values, ProtocolVersion protocolVersion) {
         super(column, op);

         assert column.type.isMultiCell();

         this.values = values;
         this.protocolVersion = protocolVersion;
      }

      public boolean appliesTo(Row row) {
         return this.isSatisfiedBy(this.rowValue(row));
      }

      private final ByteBuffer rowValue(Row row) {
         UserType userType = (UserType)this.column.type;
         Iterator<Cell> iter = ColumnCondition.getCells(row, this.column);
         return iter.hasNext()?userType.serializeForNativeProtocol(iter, this.protocolVersion):null;
      }

      private boolean isSatisfiedBy(ByteBuffer rowValue) {
         Iterator var2 = this.values.iterator();

         ByteBuffer value;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            value = (ByteBuffer)var2.next();
         } while(!compareWithOperator(this.comparisonOperator, this.column.type, value, rowValue));

         return true;
      }
   }

   private static final class UDTFieldAccessBound extends ColumnCondition.Bound {
      private final FieldIdentifier field;
      private final List<ByteBuffer> values;

      private UDTFieldAccessBound(ColumnMetadata column, FieldIdentifier field, Operator operator, List<ByteBuffer> values) {
         super(column, operator);

         assert column.type.isUDT() && field != null;

         this.field = field;
         this.values = values;
      }

      public boolean appliesTo(Row row) {
         return this.isSatisfiedBy(this.rowValue(row));
      }

      private ByteBuffer rowValue(Row row) {
         UserType userType = (UserType)this.column.type;
         Cell cell;
         if(this.column.type.isMultiCell()) {
            cell = ColumnCondition.getCell(row, this.column, userType.cellPathForField(this.field));
            return cell == null?null:cell.value();
         } else {
            cell = ColumnCondition.getCell(row, this.column);
            return cell == null?null:userType.split(cell.value())[userType.fieldPosition(this.field)];
         }
      }

      private boolean isSatisfiedBy(ByteBuffer rowValue) {
         UserType userType = (UserType)this.column.type;
         int fieldPosition = userType.fieldPosition(this.field);
         AbstractType<?> valueType = userType.fieldType(fieldPosition);
         Iterator var5 = this.values.iterator();

         ByteBuffer value;
         do {
            if(!var5.hasNext()) {
               return false;
            }

            value = (ByteBuffer)var5.next();
         } while(!compareWithOperator(this.comparisonOperator, valueType, value, rowValue));

         return true;
      }
   }

   private static final class MultiCellCollectionBound extends ColumnCondition.Bound {
      private final List<Term.Terminal> values;

      public MultiCellCollectionBound(ColumnMetadata column, Operator operator, List<Term.Terminal> values) {
         super(column, operator);

         assert column.type.isMultiCell();

         this.values = values;
      }

      public boolean appliesTo(Row row) {
         CollectionType<?> type = (CollectionType)this.column.type;
         Iterator var3 = this.values.iterator();

         while(var3.hasNext()) {
            Term.Terminal value = (Term.Terminal)var3.next();
            Iterator<Cell> iter = ColumnCondition.getCells(row, this.column);
            if(value == null) {
               if(this.comparisonOperator != Operator.EQ) {
                  if(this.comparisonOperator == Operator.NEQ) {
                     return iter.hasNext();
                  }

                  throw RequestValidations.invalidRequest("Invalid comparison with null for operator \"%s\"", new Object[]{this.comparisonOperator});
               }

               if(!iter.hasNext()) {
                  return true;
               }
            } else if(valueAppliesTo(type, iter, value, this.comparisonOperator)) {
               return true;
            }
         }

         return false;
      }

      private static boolean valueAppliesTo(CollectionType<?> type, Iterator<Cell> iter, Term.Terminal value, Operator operator) {
         if(value == null) {
            return !iter.hasNext();
         } else {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[type.kind.ordinal()]) {
            case 1:
               List<ByteBuffer> valueList = ((Lists.Value)value).elements;
               return listAppliesTo((ListType)type, iter, valueList, operator);
            case 2:
               Set<ByteBuffer> valueSet = ((Sets.Value)value).elements;
               return setAppliesTo((SetType)type, iter, valueSet, operator);
            case 3:
               Map<ByteBuffer, ByteBuffer> valueMap = ((Maps.Value)value).map;
               return mapAppliesTo((MapType)type, iter, valueMap, operator);
            default:
               throw new AssertionError();
            }
         }
      }

      private static boolean setOrListAppliesTo(AbstractType<?> type, Iterator<Cell> iter, Iterator<ByteBuffer> conditionIter, Operator operator, boolean isSet) {
         while(true) {
            if(iter.hasNext()) {
               if(conditionIter.hasNext()) {
                  ByteBuffer cellValue = isSet?((Cell)iter.next()).path().get(0):((Cell)iter.next()).value();
                  int comparison = type.compare(cellValue, (ByteBuffer)conditionIter.next());
                  if(comparison == 0) {
                     continue;
                  }

                  return ColumnCondition.evaluateComparisonWithOperator(comparison, operator);
               }

               return operator == Operator.GT || operator == Operator.GTE || operator == Operator.NEQ;
            }

            if(conditionIter.hasNext()) {
               return operator == Operator.LT || operator == Operator.LTE || operator == Operator.NEQ;
            }

            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
         }
      }

      private static boolean listAppliesTo(ListType<?> type, Iterator<Cell> iter, List<ByteBuffer> elements, Operator operator) {
         return setOrListAppliesTo(type.getElementsType(), iter, elements.iterator(), operator, false);
      }

      private static boolean setAppliesTo(SetType<?> type, Iterator<Cell> iter, Set<ByteBuffer> elements, Operator operator) {
         ArrayList<ByteBuffer> sortedElements = new ArrayList(elements);
         Collections.sort(sortedElements, type.getElementsType());
         return setOrListAppliesTo(type.getElementsType(), iter, sortedElements.iterator(), operator, true);
      }

      private static boolean mapAppliesTo(MapType<?, ?> type, Iterator<Cell> iter, Map<ByteBuffer, ByteBuffer> elements, Operator operator) {
         Iterator conditionIter = elements.entrySet().iterator();

         while(iter.hasNext()) {
            if(!conditionIter.hasNext()) {
               return operator == Operator.GT || operator == Operator.GTE || operator == Operator.NEQ;
            }

            Entry<ByteBuffer, ByteBuffer> conditionEntry = (Entry)conditionIter.next();
            Cell c = (Cell)iter.next();
            int comparison = type.getKeysType().compare(c.path().get(0), (ByteBuffer)conditionEntry.getKey());
            if(comparison != 0) {
               return ColumnCondition.evaluateComparisonWithOperator(comparison, operator);
            }

            comparison = type.getValuesType().compare(c.value(), (ByteBuffer)conditionEntry.getValue());
            if(comparison != 0) {
               return ColumnCondition.evaluateComparisonWithOperator(comparison, operator);
            }
         }

         return conditionIter.hasNext()?operator == Operator.LT || operator == Operator.LTE || operator == Operator.NEQ:operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
      }
   }

   private static final class ElementAccessBound extends ColumnCondition.Bound {
      private final ByteBuffer collectionElement;
      private final List<ByteBuffer> values;

      private ElementAccessBound(ColumnMetadata column, ByteBuffer collectionElement, Operator operator, List<ByteBuffer> values) {
         super(column, operator);
         this.collectionElement = collectionElement;
         this.values = values;
      }

      public boolean appliesTo(Row row) {
         boolean isMap = this.column.type instanceof MapType;
         if(this.collectionElement == null) {
            throw RequestValidations.invalidRequest("Invalid null value for %s element access", new Object[]{isMap?"map":"list"});
         } else {
            ByteBuffer rowValue;
            if(isMap) {
               MapType<?, ?> mapType = (MapType)this.column.type;
               rowValue = this.rowMapValue(mapType, row);
               return this.isSatisfiedBy(mapType.getKeysType(), rowValue);
            } else {
               ListType<?> listType = (ListType)this.column.type;
               rowValue = this.rowListValue(listType, row);
               return this.isSatisfiedBy(listType.getElementsType(), rowValue);
            }
         }
      }

      private ByteBuffer rowMapValue(MapType<?, ?> type, Row row) {
         Cell cell;
         if(this.column.type.isMultiCell()) {
            cell = ColumnCondition.getCell(row, this.column, CellPath.create(this.collectionElement));
            return cell == null?null:cell.value();
         } else {
            cell = ColumnCondition.getCell(row, this.column);
            return cell == null?null:type.getSerializer().getSerializedValue(cell.value(), this.collectionElement, type.getKeysType());
         }
      }

      private ByteBuffer rowListValue(ListType<?> type, Row row) {
         if(this.column.type.isMultiCell()) {
            return cellValueAtIndex(ColumnCondition.getCells(row, this.column), getListIndex(this.collectionElement));
         } else {
            Cell cell = ColumnCondition.getCell(row, this.column);
            return cell == null?null:type.getSerializer().getElement(cell.value(), getListIndex(this.collectionElement));
         }
      }

      private static ByteBuffer cellValueAtIndex(Iterator<Cell> iter, int index) {
         int adv = Iterators.advance(iter, index);
         return adv == index && iter.hasNext()?((Cell)iter.next()).value():null;
      }

      private boolean isSatisfiedBy(AbstractType<?> valueType, ByteBuffer rowValue) {
         Iterator var3 = this.values.iterator();

         ByteBuffer value;
         do {
            if(!var3.hasNext()) {
               return false;
            }

            value = (ByteBuffer)var3.next();
         } while(!compareWithOperator(this.comparisonOperator, valueType, value, rowValue));

         return true;
      }

      public ByteBuffer getCollectionElementValue() {
         return this.collectionElement;
      }

      private static int getListIndex(ByteBuffer collectionElement) {
         int idx = ByteBufferUtil.toInt(collectionElement);
         RequestValidations.checkFalse(idx < 0, "Invalid negative list index %d", Integer.valueOf(idx));
         return idx;
      }
   }

   private static final class SimpleBound extends ColumnCondition.Bound {
      private final List<ByteBuffer> values;

      private SimpleBound(ColumnMetadata column, Operator operator, List<ByteBuffer> values) {
         super(column, operator);
         this.values = values;
      }

      public boolean appliesTo(Row row) {
         return this.isSatisfiedBy(this.rowValue(row));
      }

      private ByteBuffer rowValue(Row row) {
         Cell c = ColumnCondition.getCell(row, this.column);
         return c == null?null:c.value();
      }

      private boolean isSatisfiedBy(ByteBuffer rowValue) {
         Iterator var2 = this.values.iterator();

         ByteBuffer value;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            value = (ByteBuffer)var2.next();
         } while(!compareWithOperator(this.comparisonOperator, this.column.type, value, rowValue));

         return true;
      }
   }

   public abstract static class Bound {
      public final ColumnMetadata column;
      public final Operator comparisonOperator;

      protected Bound(ColumnMetadata column, Operator operator) {
         this.column = column;
         this.comparisonOperator = operator.isIN()?Operator.EQ:operator;
      }

      public abstract boolean appliesTo(Row var1);

      public ByteBuffer getCollectionElementValue() {
         return null;
      }

      protected static boolean compareWithOperator(Operator operator, AbstractType<?> type, ByteBuffer value, ByteBuffer otherValue) {
         if(value == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            throw RequestValidations.invalidRequest("Invalid 'unset' value in condition");
         } else if(value == null) {
            switch(null.$SwitchMap$org$apache$cassandra$cql3$Operator[operator.ordinal()]) {
            case 1:
               return otherValue == null;
            case 2:
               return otherValue != null;
            default:
               throw RequestValidations.invalidRequest("Invalid comparison with null for operator \"%s\"", new Object[]{operator});
            }
         } else {
            return otherValue == null?operator == Operator.NEQ:operator.isSatisfiedBy(type, otherValue, value);
         }
      }
   }

   private static final class UDTFieldCondition extends ColumnCondition {
      private final FieldIdentifier udtField;

      public UDTFieldCondition(ColumnMetadata column, FieldIdentifier udtField, Operator op, Terms values) {
         super(column, op, values, null);

         assert udtField != null;

         this.udtField = udtField;
      }

      public ColumnCondition.Bound bind(QueryOptions options) {
         return new ColumnCondition.UDTFieldAccessBound(this.column, this.udtField, this.operator, this.bindAndGetTerms(options), null);
      }
   }

   private static class CollectionElementCondition extends ColumnCondition {
      private final Term collectionElement;

      public CollectionElementCondition(ColumnMetadata column, Term collectionElement, Operator op, Terms values) {
         super(column, op, values, null);
         this.collectionElement = collectionElement;
      }

      public void addFunctionsTo(List<Function> functions) {
         this.collectionElement.addFunctionsTo(functions);
         super.addFunctionsTo(functions);
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
         this.collectionElement.collectMarkerSpecification(boundNames);
         super.collectMarkerSpecification(boundNames);
      }

      public ColumnCondition.Bound bind(QueryOptions options) {
         return new ColumnCondition.ElementAccessBound(this.column, this.collectionElement.bindAndGet(options), this.operator, this.bindAndGetTerms(options), null);
      }
   }

   private static final class SimpleColumnCondition extends ColumnCondition {
      public SimpleColumnCondition(ColumnMetadata column, Operator op, Terms values) {
         super(column, op, values, null);
      }

      public ColumnCondition.Bound bind(QueryOptions options) {
         return (ColumnCondition.Bound)(this.column.type.isCollection() && this.column.type.isMultiCell()?new ColumnCondition.MultiCellCollectionBound(this.column, this.operator, this.bindTerms(options)):(this.column.type.isUDT() && this.column.type.isMultiCell()?new ColumnCondition.MultiCellUdtBound(this.column, this.operator, this.bindAndGetTerms(options), options.getProtocolVersion(), null):new ColumnCondition.SimpleBound(this.column, this.operator, this.bindAndGetTerms(options), null)));
      }
   }
}
