package org.apache.cassandra.cql3.selection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.AggregateFcts;
import org.apache.cassandra.cql3.functions.CastFcts;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionResolver;
import org.apache.cassandra.cql3.functions.OperationFcts;
import org.apache.cassandra.cql3.functions.ToJsonFct;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.text.StrBuilder;

public interface Selectable extends AssignmentTestable {
   Selector.Factory newSelectorFactory(TableMetadata var1, AbstractType<?> var2, List<ColumnMetadata> var3, VariableSpecifications var4);

   AbstractType<?> getExactTypeIfKnown(String var1);

   boolean selectColumns(Predicate<ColumnMetadata> var1);

   public static boolean selectColumns(List<Selectable> selectables, Predicate<ColumnMetadata> predicate) {
      Iterator var2 = selectables.iterator();

      Selectable selectable;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         selectable = (Selectable)var2.next();
      } while(!selectable.selectColumns(predicate));

      return true;
   }

   default boolean processesSelection() {
      return true;
   }

   default void validateType(TableMetadata table, AbstractType<?> type) {
      ColumnSpecification receiver = new ColumnSpecification(table.keyspace, table.name, new ColumnIdentifier(this.toString(), true), type);
      if(!this.testAssignment(table.keyspace, receiver).isAssignable()) {
         throw RequestValidations.invalidRequest("%s is not of the expected type: %s", new Object[]{this, type.asCQL3Type()});
      }
   }

   default AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
      AbstractType<?> type = this.getExactTypeIfKnown(keyspace);
      return type == null?AssignmentTestable.TestResult.NOT_ASSIGNABLE:type.testAssignment(keyspace, receiver);
   }

   default int addAndGetIndex(ColumnMetadata def, List<ColumnMetadata> l) {
      int idx = l.indexOf(def);
      if(idx < 0) {
         idx = l.size();
         l.add(def);
      }

      return idx;
   }

   default ColumnSpecification specForElementOrSlice(Selectable selected, ColumnSpecification receiver, String selectionType) {
      switch (((CollectionType)receiver.type).kind) {
         case LIST: {
            throw new InvalidRequestException(String.format("%s selection is only allowed on sets and maps, but %s is a list", selectionType, selected));
         }
         case SET: {
            return Sets.valueSpecOf(receiver);
         }
         case MAP: {
            return Maps.keySpecOf(receiver);
         }
      }
      throw new AssertionError();
   }

   public static class WithSliceSelection implements Selectable {
      public final Selectable selected;
      public final Term.Raw from;
      public final Term.Raw to;

      public WithSliceSelection(Selectable selected, Term.Raw from, Term.Raw to) {
         this.selected = selected;
         this.from = from;
         this.to = to;
      }

      public String toString() {
         return String.format("%s[%s..%s]", new Object[]{this.selected, this.from == null?"":this.from, this.to == null?"":this.to});
      }

      public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         Selector.Factory factory = this.selected.newSelectorFactory(cfm, expectedType, defs, boundNames);
         ColumnSpecification receiver = factory.getColumnSpecification(cfm);
         if(!(receiver.type instanceof CollectionType)) {
            throw new InvalidRequestException(String.format("Invalid slice selection: %s of type %s is not a collection", new Object[]{this.selected, receiver.type.asCQL3Type()}));
         } else {
            ColumnSpecification boundSpec = this.specForElementOrSlice(this.selected, receiver, "Slice");
            Term f = this.from == null?Constants.UNSET_VALUE:this.from.prepare(cfm.keyspace, boundSpec);
            Term t = this.to == null?Constants.UNSET_VALUE:this.to.prepare(cfm.keyspace, boundSpec);
            ((Term)f).collectMarkerSpecification(boundNames);
            ((Term)t).collectMarkerSpecification(boundNames);
            return ElementsSelector.newSliceFactory(this.toString(), factory, (CollectionType)receiver.type, (Term)f, (Term)t);
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         AbstractType<?> selectedType = this.selected.getExactTypeIfKnown(keyspace);
         return selectedType != null && selectedType instanceof CollectionType?selectedType:null;
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return this.selected.selectColumns(predicate);
      }

      public static class Raw extends Selectable.Raw {
         private final Selectable.Raw selected;
         private final Term.Raw from;
         private final Term.Raw to;

         public Raw(Selectable.Raw selected, Term.Raw from, Term.Raw to) {
            this.selected = selected;
            this.from = from;
            this.to = to;
         }

         public Selectable.WithSliceSelection prepare(TableMetadata cfm) {
            return new Selectable.WithSliceSelection(this.selected.prepare(cfm), this.from, this.to);
         }
      }
   }

   public static class WithElementSelection implements Selectable {
      public final Selectable selected;
      public final Term.Raw element;

      public WithElementSelection(Selectable selected, Term.Raw element) {
         assert element != null;

         this.selected = selected;
         this.element = element;
      }

      public String toString() {
         return String.format("%s[%s]", new Object[]{this.selected, this.element});
      }

      public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         Selector.Factory factory = this.selected.newSelectorFactory(cfm, (AbstractType)null, defs, boundNames);
         ColumnSpecification receiver = factory.getColumnSpecification(cfm);
         if(!(receiver.type instanceof CollectionType)) {
            throw new InvalidRequestException(String.format("Invalid element selection: %s is of type %s is not a collection", new Object[]{this.selected, receiver.type.asCQL3Type()}));
         } else {
            ColumnSpecification boundSpec = this.specForElementOrSlice(this.selected, receiver, "Element");
            Term elt = this.element.prepare(cfm.keyspace, boundSpec);
            elt.collectMarkerSpecification(boundNames);
            return ElementsSelector.newElementFactory(this.toString(), factory, (CollectionType)receiver.type, elt);
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         AbstractType<?> selectedType = this.selected.getExactTypeIfKnown(keyspace);
         return selectedType != null && selectedType instanceof CollectionType?ElementsSelector.valueType((CollectionType)selectedType):null;
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return this.selected.selectColumns(predicate);
      }

      public static class Raw extends Selectable.Raw {
         private final Selectable.Raw selected;
         private final Term.Raw element;

         public Raw(Selectable.Raw selected, Term.Raw element) {
            this.selected = selected;
            this.element = element;
         }

         public Selectable.WithElementSelection prepare(TableMetadata cfm) {
            return new Selectable.WithElementSelection(this.selected.prepare(cfm), this.element);
         }
      }
   }

   public static final class RawIdentifier extends Selectable.Raw {
      private final String text;
      private final boolean quoted;

      public static Selectable.Raw forUnquoted(String text) {
         return new Selectable.RawIdentifier(text, false);
      }

      public static Selectable.Raw forQuoted(String text) {
         return new Selectable.RawIdentifier(text, true);
      }

      private RawIdentifier(String text, boolean quoted) {
         this.text = text;
         this.quoted = quoted;
      }

      public Selectable prepare(TableMetadata cfm) {
         ColumnMetadata.Raw raw = this.quoted?ColumnMetadata.Raw.forQuoted(this.text):ColumnMetadata.Raw.forUnquoted(this.text);
         return raw.prepare(cfm);
      }

      public FieldIdentifier toFieldIdentifier() {
         return this.quoted?FieldIdentifier.forQuoted(this.text):FieldIdentifier.forUnquoted(this.text);
      }

      public String toString() {
         return this.text;
      }
   }

   public static class WithTypeHint implements Selectable {
      private final String typeName;
      private final AbstractType<?> type;
      private final Selectable selectable;

      public WithTypeHint(String typeName, AbstractType<?> type, Selectable selectable) {
         this.typeName = typeName;
         this.type = type;
         this.selectable = selectable;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return receiver.type.equals(this.type)?AssignmentTestable.TestResult.EXACT_MATCH:(receiver.type.isValueCompatibleWith(this.type)?AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE:AssignmentTestable.TestResult.NOT_ASSIGNABLE);
      }

      public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         ColumnSpecification receiver = new ColumnSpecification(cfm.keyspace, cfm.name, new ColumnIdentifier(this.toString(), true), this.type);
         if(!this.selectable.testAssignment(cfm.keyspace, receiver).isAssignable()) {
            throw new InvalidRequestException(String.format("Cannot assign value %s to %s of type %s", new Object[]{this, receiver.name, receiver.type.asCQL3Type()}));
         } else {
            final Selector.Factory factory = this.selectable.newSelectorFactory(cfm, this.type, defs, boundNames);
            return new ForwardingFactory() {
               protected Selector.Factory delegate() {
                  return factory;
               }

               protected AbstractType<?> getReturnType() {
                  return WithTypeHint.this.type;
               }

               protected String getColumnName() {
                  return String.format("(%s)%s", new Object[]{WithTypeHint.this.typeName, factory.getColumnName()});
               }
            };
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return this.type;
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return this.selectable.selectColumns(predicate);
      }

      public String toString() {
         return String.format("(%s)%s", new Object[]{this.typeName, this.selectable});
      }

      public static class Raw extends Selectable.Raw {
         private final CQL3Type.Raw typeRaw;
         private final Selectable.Raw raw;

         public Raw(CQL3Type.Raw typeRaw, Selectable.Raw raw) {
            this.typeRaw = typeRaw;
            this.raw = raw;
         }

         public Selectable prepare(TableMetadata cfm) {
            Selectable selectable = this.raw.prepare(cfm);
            AbstractType<?> type = this.typeRaw.prepare(cfm.keyspace).getType();
            if(type.isFreezable()) {
               type = type.freeze();
            }

            return new Selectable.WithTypeHint(this.typeRaw.toString(), type, selectable);
         }
      }
   }

   public static class WithMapOrUdt implements Selectable {
      private final TableMetadata cfm;
      private final List<Pair<Selectable.Raw, Selectable.Raw>> raws;

      public WithMapOrUdt(TableMetadata cfm, List<Pair<Selectable.Raw, Selectable.Raw>> raws) {
         this.cfm = cfm;
         this.raws = raws;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return receiver.type.isUDT()?UserTypes.testUserTypeAssignment(receiver, this.getUdtFields((UserType)receiver.type)):Maps.testMapAssignment(receiver, this.getMapEntries(this.cfm));
      }

      public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         AbstractType<?> type = this.getExactTypeIfKnown(cfm.keyspace);
         if(type == null) {
            type = expectedType;
            if(expectedType == null) {
               throw RequestValidations.invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)", new Object[]{this});
            }

            this.validateType(cfm, expectedType);
         }

         return type.isUDT()?this.newUdtSelectorFactory(cfm, expectedType, defs, boundNames):this.newMapSelectorFactory(cfm, defs, boundNames, type);
      }

      private Selector.Factory newMapSelectorFactory(TableMetadata cfm, List<ColumnMetadata> defs, VariableSpecifications boundNames, AbstractType<?> type) {
         MapType<?, ?> mapType = (MapType)type;
         if(mapType.getKeysType() == DurationType.instance) {
            throw RequestValidations.invalidRequest("Durations are not allowed as map keys: %s", new Object[]{mapType.asCQL3Type()});
         } else {
            return MapSelector.newFactory(type, (List)this.getMapEntries(cfm).stream().map((p) -> {
               return Pair.create(((Selectable)p.left).newSelectorFactory(cfm, mapType.getKeysType(), defs, boundNames), ((Selectable)p.right).newSelectorFactory(cfm, mapType.getValuesType(), defs, boundNames));
            }).collect(Collectors.toList()));
         }
      }

      private Selector.Factory newUdtSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         UserType ut = (UserType)expectedType;
         Map<FieldIdentifier, Selector.Factory> factories = new LinkedHashMap(ut.size());
         Iterator var7 = this.raws.iterator();

         while(var7.hasNext()) {
            Pair<Selectable.Raw, Selectable.Raw> raw = (Pair)var7.next();
            if(!(raw.left instanceof Selectable.RawIdentifier)) {
               throw RequestValidations.invalidRequest("%s is not a valid field identifier of type %s ", new Object[]{raw.left, ut.getNameAsString()});
            }

            FieldIdentifier fieldName = ((Selectable.RawIdentifier)raw.left).toFieldIdentifier();
            int fieldPosition = ut.fieldPosition(fieldName);
            if(fieldPosition == -1) {
               throw RequestValidations.invalidRequest("Unknown field '%s' in value of user defined type %s", new Object[]{fieldName, ut.getNameAsString()});
            }

            AbstractType<?> fieldType = ut.fieldType(fieldPosition);
            factories.put(fieldName, ((Selectable.Raw)raw.right).prepare(cfm).newSelectorFactory(cfm, fieldType, defs, boundNames));
         }

         return UserTypeSelector.newFactory(expectedType, factories);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         Iterator var2 = this.raws.iterator();

         Pair raw;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            raw = (Pair)var2.next();
            if(!(raw.left instanceof Selectable.RawIdentifier) && ((Selectable.Raw)raw.left).prepare(this.cfm).selectColumns(predicate)) {
               return true;
            }
         } while(((Selectable.Raw)raw.right).prepare(this.cfm).selectColumns(predicate));

         return true;
      }

      public String toString() {
         return (String)this.raws.stream().map((p) -> {
            return String.format("%s: %s", new Object[]{p.left instanceof Selectable.RawIdentifier?p.left:((Selectable.Raw)p.left).prepare(this.cfm), ((Selectable.Raw)p.right).prepare(this.cfm)});
         }).collect(Collectors.joining(", ", "{", "}"));
      }

      private List<Pair<Selectable, Selectable>> getMapEntries(TableMetadata cfm) {
         return (List)this.raws.stream().map((p) -> {
            return Pair.create(((Selectable.Raw)p.left).prepare(cfm), ((Selectable.Raw)p.right).prepare(cfm));
         }).collect(Collectors.toList());
      }

      private Map<FieldIdentifier, Selectable> getUdtFields(UserType ut) {
         Map<FieldIdentifier, Selectable> fields = new LinkedHashMap(ut.size());
         Iterator var3 = this.raws.iterator();

         while(var3.hasNext()) {
            Pair<Selectable.Raw, Selectable.Raw> raw = (Pair)var3.next();
            if(!(raw.left instanceof Selectable.RawIdentifier)) {
               throw RequestValidations.invalidRequest("%s is not a valid field identifier of type %s ", new Object[]{raw.left, ut.getNameAsString()});
            }

            FieldIdentifier fieldName = ((Selectable.RawIdentifier)raw.left).toFieldIdentifier();
            int fieldPosition = ut.fieldPosition(fieldName);
            if(fieldPosition == -1) {
               throw RequestValidations.invalidRequest("Unknown field '%s' in value of user defined type %s", new Object[]{fieldName, ut.getNameAsString()});
            }

            fields.put(fieldName, ((Selectable.Raw)raw.right).prepare(this.cfm));
         }

         return fields;
      }

      public static class Raw extends Selectable.Raw {
         private final List<Pair<Selectable.Raw, Selectable.Raw>> raws;

         public Raw(List<Pair<Selectable.Raw, Selectable.Raw>> raws) {
            this.raws = raws;
         }

         public Selectable prepare(TableMetadata cfm) {
            return new Selectable.WithMapOrUdt(cfm, this.raws);
         }
      }
   }

   public static class WithSet implements Selectable {
      private final List<Selectable> selectables;

      public WithSet(List<Selectable> selectables) {
         this.selectables = selectables;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return Sets.testSetAssignment(receiver, this.selectables);
      }

      public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         AbstractType<?> type = this.getExactTypeIfKnown(cfm.keyspace);
         if(type == null) {
            type = expectedType;
            if(expectedType == null) {
               throw RequestValidations.invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)", new Object[]{this});
            }

            this.validateType(cfm, expectedType);
         }

         if(type instanceof MapType) {
            return MapSelector.newFactory(type, Collections.emptyList());
         } else {
            SetType<?> setType = (SetType)type;
            if(setType.getElementsType() == DurationType.instance) {
               throw RequestValidations.invalidRequest("Durations are not allowed inside sets: %s", new Object[]{setType.asCQL3Type()});
            } else {
               List<AbstractType<?>> expectedTypes = new ArrayList(this.selectables.size());
               int i = 0;

               for(int m = this.selectables.size(); i < m; ++i) {
                  expectedTypes.add(setType.getElementsType());
               }

               SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(this.selectables, expectedTypes, cfm, defs, boundNames);
               return SetSelector.newFactory(type, factories);
            }
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return Sets.getExactSetTypeIfKnown(this.selectables, (p) -> {
            return p.getExactTypeIfKnown(keyspace);
         });
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return Selectable.selectColumns(this.selectables, predicate);
      }

      public String toString() {
         return Sets.setToString(this.selectables);
      }

      public static class Raw extends Selectable.Raw {
         private final List<Selectable.Raw> raws;

         public Raw(List<Selectable.Raw> raws) {
            this.raws = raws;
         }

         public Selectable prepare(TableMetadata cfm) {
            return new Selectable.WithSet((List)this.raws.stream().map((p) -> {
               return p.prepare(cfm);
            }).collect(Collectors.toList()));
         }
      }
   }

   public static class WithList implements Selectable {
      private final List<Selectable> selectables;

      public WithList(List<Selectable> selectables) {
         this.selectables = selectables;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return Lists.testListAssignment(receiver, this.selectables);
      }

      public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         AbstractType<?> type = this.getExactTypeIfKnown(cfm.keyspace);
         if(type == null) {
            type = expectedType;
            if(expectedType == null) {
               throw RequestValidations.invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)", new Object[]{this});
            }

            this.validateType(cfm, expectedType);
         }

         ListType<?> listType = (ListType)type;
         List<AbstractType<?>> expectedTypes = new ArrayList(this.selectables.size());
         int i = 0;

         for(int m = this.selectables.size(); i < m; ++i) {
            expectedTypes.add(listType.getElementsType());
         }

         SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(this.selectables, expectedTypes, cfm, defs, boundNames);
         return ListSelector.newFactory(type, factories);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return Lists.getExactListTypeIfKnown(this.selectables, (p) -> {
            return p.getExactTypeIfKnown(keyspace);
         });
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return Selectable.selectColumns(this.selectables, predicate);
      }

      public String toString() {
         return Lists.listToString(this.selectables);
      }

      public static class Raw extends Selectable.Raw {
         private final List<Selectable.Raw> raws;

         public Raw(List<Selectable.Raw> raws) {
            this.raws = raws;
         }

         public Selectable prepare(TableMetadata cfm) {
            return new Selectable.WithList((List)this.raws.stream().map((p) -> {
               return p.prepare(cfm);
            }).collect(Collectors.toList()));
         }
      }
   }

   public static class BetweenParenthesesOrWithTuple implements Selectable {
      private final List<Selectable> selectables;

      public BetweenParenthesesOrWithTuple(List<Selectable> selectables) {
         this.selectables = selectables;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return this.selectables.size() == 1 && !receiver.type.isTuple()?((Selectable)this.selectables.get(0)).testAssignment(keyspace, receiver):Tuples.testTupleAssignment(receiver, this.selectables);
      }

      public Selector.Factory newSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         AbstractType<?> type = this.getExactTypeIfKnown(cfm.keyspace);
         if(type == null) {
            type = expectedType;
            if(expectedType == null) {
               throw RequestValidations.invalidRequest("Cannot infer type for term %s in selection clause (try using a cast to force a type)", new Object[]{this});
            }

            this.validateType(cfm, expectedType);
         }

         return this.selectables.size() == 1 && !type.isTuple()?this.newBetweenParenthesesSelectorFactory(cfm, expectedType, defs, boundNames):this.newTupleSelectorFactory(cfm, (TupleType)type, defs, boundNames);
      }

      private Selector.Factory newBetweenParenthesesSelectorFactory(TableMetadata cfm, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         Selectable selectable = (Selectable)this.selectables.get(0);
         final Selector.Factory factory = selectable.newSelectorFactory(cfm, expectedType, defs, boundNames);
         return new ForwardingFactory() {
            protected Selector.Factory delegate() {
               return factory;
            }

            protected String getColumnName() {
               return String.format("(%s)", new Object[]{factory.getColumnName()});
            }
         };
      }

      private Selector.Factory newTupleSelectorFactory(TableMetadata cfm, TupleType tupleType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(this.selectables, tupleType.allTypes(), cfm, defs, boundNames);
         return TupleSelector.newFactory(tupleType, factories);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return this.selectables.size() == 1?null:Tuples.getExactTupleTypeIfKnown(this.selectables, (p) -> {
            return p.getExactTypeIfKnown(keyspace);
         });
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return Selectable.selectColumns(this.selectables, predicate);
      }

      public String toString() {
         return Tuples.tupleToString(this.selectables);
      }

      public static class Raw extends Selectable.Raw {
         private final List<Selectable.Raw> raws;

         public Raw(List<Selectable.Raw> raws) {
            this.raws = raws;
         }

         public Selectable prepare(TableMetadata cfm) {
            return new Selectable.BetweenParenthesesOrWithTuple((List)this.raws.stream().map((p) -> {
               return p.prepare(cfm);
            }).collect(Collectors.toList()));
         }
      }
   }

   public static class WithFieldSelection implements Selectable {
      public final Selectable selected;
      public final FieldIdentifier field;

      public WithFieldSelection(Selectable selected, FieldIdentifier field) {
         this.selected = selected;
         this.field = field;
      }

      public String toString() {
         return String.format("%s.%s", new Object[]{this.selected, this.field});
      }

      public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         AbstractType<?> expectedUdtType = null;
         if(this.selected instanceof Selectable.BetweenParenthesesOrWithTuple) {
            Selectable.BetweenParenthesesOrWithTuple betweenParentheses = (Selectable.BetweenParenthesesOrWithTuple)this.selected;
            expectedUdtType = ((Selectable)betweenParentheses.selectables.get(0)).getExactTypeIfKnown(table.keyspace);
         }

         Selector.Factory factory = this.selected.newSelectorFactory(table, expectedUdtType, defs, boundNames);
         AbstractType<?> type = factory.getReturnType();
         if(!type.isUDT()) {
            throw new InvalidRequestException(String.format("Invalid field selection: %s of type %s is not a user type", new Object[]{this.selected, type.asCQL3Type()}));
         } else {
            UserType ut = (UserType)type;
            int fieldIndex = ut.fieldPosition(this.field);
            if(fieldIndex == -1) {
               throw new InvalidRequestException(String.format("%s of type %s has no field %s", new Object[]{this.selected, type.asCQL3Type(), this.field}));
            } else {
               return FieldSelector.newFactory(ut, fieldIndex, factory);
            }
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         AbstractType<?> selectedType = this.selected.getExactTypeIfKnown(keyspace);
         if(selectedType != null && selectedType instanceof UserType) {
            UserType ut = (UserType)selectedType;
            int fieldIndex = ut.fieldPosition(this.field);
            return fieldIndex == -1?null:ut.fieldType(fieldIndex);
         } else {
            return null;
         }
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return this.selected.selectColumns(predicate);
      }

      public static class Raw extends Selectable.Raw {
         private final Selectable.Raw selected;
         private final FieldIdentifier field;

         public Raw(Selectable.Raw selected, FieldIdentifier field) {
            this.selected = selected;
            this.field = field;
         }

         public Selectable.WithFieldSelection prepare(TableMetadata table) {
            return new Selectable.WithFieldSelection(this.selected.prepare(table), this.field);
         }
      }
   }

   public static class WithCast implements Selectable {
      private final CQL3Type type;
      private final Selectable arg;

      public WithCast(Selectable arg, CQL3Type type) {
         this.arg = arg;
         this.type = type;
      }

      public String toString() {
         return String.format("cast(%s as %s)", new Object[]{this.arg, this.type.toString().toLowerCase()});
      }

      public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         List<Selectable> args = Collections.singletonList(this.arg);
         SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(args, (List)null, table, defs, boundNames);
         Selector.Factory factory = factories.get(0);
         if(this.type.getType().equals(factory.getReturnType())) {
            return factory;
         } else {
            FunctionName name = FunctionName.nativeFunction(CastFcts.getFunctionName(this.type));
            org.apache.cassandra.cql3.functions.Function fun = FunctionResolver.get(table.keyspace, name, args, table.keyspace, table.name, (AbstractType)null);
            if(fun == null) {
               throw new InvalidRequestException(String.format("%s cannot be cast to %s", new Object[]{((ColumnMetadata)defs.get(0)).name, this.type}));
            } else {
               return AbstractFunctionSelector.newFactory(fun, factories);
            }
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return this.type.getType();
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return this.arg.selectColumns(predicate);
      }

      public static class Raw extends Selectable.Raw {
         private final CQL3Type type;
         private final Selectable.Raw arg;

         public Raw(Selectable.Raw arg, CQL3Type type) {
            this.arg = arg;
            this.type = type;
         }

         public Selectable.WithCast prepare(TableMetadata table) {
            return new Selectable.WithCast(this.arg.prepare(table), this.type);
         }
      }
   }

   public static class WithToJSonFunction implements Selectable {
      public final List<Selectable> args;

      private WithToJSonFunction(List<Selectable> args) {
         this.args = args;
      }

      public String toString() {
         return (new StrBuilder()).append(ToJsonFct.NAME).append("(").appendWithSeparators(this.args, ", ").append(")").toString();
      }

      public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(this.args, (List)null, table, defs, boundNames);
         org.apache.cassandra.cql3.functions.Function fun = ToJsonFct.getInstance(factories.getReturnTypes());
         return AbstractFunctionSelector.newFactory(fun, factories);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return UTF8Type.instance;
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return Selectable.selectColumns(this.args, predicate);
      }
   }

   public static class WithFunction implements Selectable {
      public final org.apache.cassandra.cql3.functions.Function function;
      public final List<Selectable> args;

      public WithFunction(org.apache.cassandra.cql3.functions.Function function, List<Selectable> args) {
         this.function = function;
         this.args = args;
      }

      public org.apache.cassandra.cql3.functions.Function getFunction() {
         return this.function;
      }

      public List<Selectable> getArguments() {
         return this.args;
      }

      public String toString() {
         return this.function.columnName((List)this.args.stream().map(Object::toString).collect(Collectors.toList()));
      }

      public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         SelectorFactories factories = SelectorFactories.createFactoriesAndCollectColumnDefinitions(this.args, this.function.argTypes(), table, defs, boundNames);
         return AbstractFunctionSelector.newFactory(this.function, factories);
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return Selectable.selectColumns(this.args, predicate);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return this.function.returnType();
      }

      public static class Raw extends Selectable.Raw {
         public final FunctionName functionName;
         public final List<Selectable.Raw> args;

         public Raw(FunctionName functionName, List<Selectable.Raw> args) {
            this.functionName = functionName;
            this.args = args;
         }

         public static Selectable.WithFunction.Raw newCountRowsFunction() {
            return new Selectable.WithFunction.Raw(AggregateFcts.countRowsFunction.name(), Collections.emptyList());
         }

         public static Selectable.WithFunction.Raw newOperation(char operator, Selectable.Raw left, Selectable.Raw right) {
            return new Selectable.WithFunction.Raw(OperationFcts.getFunctionNameFromOperator(operator), Arrays.asList(new Selectable.Raw[]{left, right}));
         }

         public static Selectable.WithFunction.Raw newNegation(Selectable.Raw arg) {
            return new Selectable.WithFunction.Raw(FunctionName.nativeFunction("_negate"), Collections.singletonList(arg));
         }

         public Selectable prepare(TableMetadata table) {
            List<Selectable> preparedArgs = new ArrayList<Selectable>(this.args.size());
            for (final Selectable.Raw arg : this.args) {
               preparedArgs.add(arg.prepare(table));
            }
            FunctionName name = this.functionName;
            if (this.functionName.equalsNativeFunction(ToJsonFct.NAME)) {
               return new WithToJSonFunction((List)preparedArgs);
            }
            if (this.functionName.equalsNativeFunction(FunctionName.nativeFunction("count")) &&
                    preparedArgs.size() == 1 && preparedArgs.get(0) instanceof WithTerm &&
                    ((Selectable.WithTerm)((List)preparedArgs).get(0)).rawTerm instanceof Constants.Literal) {
               name = AggregateFcts.countRowsFunction.name();
               preparedArgs = Collections.emptyList();
            }
            org.apache.cassandra.cql3.functions.Function fun = FunctionResolver.get(table.keyspace, name, preparedArgs, table.keyspace, table.name, null);
            if (fun == null) {
               throw new InvalidRequestException(String.format("Unknown function '%s'", this.functionName));
            }
            if (fun.returnType() == null) {
               throw new InvalidRequestException(String.format("Unknown function %s called in selection clause", this.functionName));
            }
            return new WithFunction(fun, preparedArgs);
         }
      }
   }

   public static class WritetimeOrTTL implements Selectable {
      public final ColumnMetadata column;
      public final boolean isWritetime;

      public WritetimeOrTTL(ColumnMetadata column, boolean isWritetime) {
         this.column = column;
         this.isWritetime = isWritetime;
      }

      public String toString() {
         return (this.isWritetime?"writetime":"ttl") + "(" + this.column.name + ")";
      }

      public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) {
         if(this.column.isPrimaryKeyColumn()) {
            throw new InvalidRequestException(String.format("Cannot use selection function %s on PRIMARY KEY part %s", new Object[]{this.isWritetime?"writeTime":"ttl", this.column.name}));
         } else if(this.column.type.isCollection()) {
            throw new InvalidRequestException(String.format("Cannot use selection function %s on collections", new Object[]{this.isWritetime?"writeTime":"ttl"}));
         } else {
            return WritetimeOrTTLSelector.newFactory(this.column, this.addAndGetIndex(this.column, defs), this.isWritetime);
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return (AbstractType)(this.isWritetime?LongType.instance:Int32Type.instance);
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return predicate.test(this.column);
      }

      public static class Raw extends Selectable.Raw {
         private final ColumnMetadata.Raw id;
         private final boolean isWritetime;

         public Raw(ColumnMetadata.Raw id, boolean isWritetime) {
            this.id = id;
            this.isWritetime = isWritetime;
         }

         public Selectable.WritetimeOrTTL prepare(TableMetadata table) {
            return new Selectable.WritetimeOrTTL(this.id.prepare(table), this.isWritetime);
         }
      }
   }

   public static class WithTerm implements Selectable {
      private static final ColumnIdentifier bindMarkerNameInSelection = new ColumnIdentifier("[selection]", true);
      private final Term.Raw rawTerm;

      public WithTerm(Term.Raw rawTerm) {
         this.rawTerm = rawTerm;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return this.rawTerm.testAssignment(keyspace, receiver);
      }

      public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) throws InvalidRequestException {
         AbstractType<?> type = this.getExactTypeIfKnown(table.keyspace);
         if(type == null) {
            type = expectedType;
            if(expectedType == null) {
               throw new InvalidRequestException("Cannot infer type for term " + this + " in selection clause (try using a cast to force a type)");
            }

            this.validateType(table, expectedType);
         }

         Term term = this.getTerm(table, bindMarkerNameInSelection, type);
         term.collectMarkerSpecification(boundNames);
         return TermSelector.newFactory(this.rawTerm.getText(), term, type);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return this.rawTerm.getExactTypeIfKnown(keyspace);
      }

      public boolean selectColumns(Predicate<ColumnMetadata> predicate) {
         return false;
      }

      public String toString() {
         return this.rawTerm.getText();
      }

      public Term getTerm(TableMetadata table, ColumnIdentifier identifier, AbstractType<?> type) {
         return this.rawTerm.prepare(table.keyspace, new ColumnSpecification(table.keyspace, table.name, bindMarkerNameInSelection, type));
      }

      public static class Raw extends Selectable.Raw {
         private final Term.Raw term;

         public Raw(Term.Raw term) {
            this.term = term;
         }

         public Selectable prepare(TableMetadata table) {
            return new Selectable.WithTerm(this.term);
         }
      }
   }

   public abstract static class Raw {
      public Raw() {
      }

      public abstract Selectable prepare(TableMetadata var1);
   }
}
