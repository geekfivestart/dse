package org.apache.cassandra.db.filter;

import io.reactivex.functions.Predicate;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RowFilter implements Iterable<RowFilter.Expression> {
   private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);
   public static final Versioned<ReadVerbs.ReadVersion, RowFilter.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new RowFilter.Serializer(x$0, null);
   });
   public static final RowFilter NONE = new RowFilter.CQLFilter(Collections.emptyList(), null);
   protected final List<RowFilter.Expression> expressions;

   protected RowFilter(List<RowFilter.Expression> expressions) {
      this.expressions = expressions;
   }

   public static RowFilter create() {
      return new RowFilter.CQLFilter(new ArrayList(), null);
   }

   public static RowFilter create(int capacity) {
      return new RowFilter.CQLFilter(new ArrayList(capacity), null);
   }

   public RowFilter.SimpleExpression add(ColumnMetadata def, Operator op, ByteBuffer value) {
      RowFilter.SimpleExpression expression = new RowFilter.SimpleExpression(def, op, value);
      this.add(expression);
      return expression;
   }

   public void addMapEquality(ColumnMetadata def, ByteBuffer key, Operator op, ByteBuffer value) {
      this.add(new RowFilter.MapEqualityExpression(def, key, op, value));
   }

   public void addCustomIndexExpression(TableMetadata metadata, IndexMetadata targetIndex, ByteBuffer value) {
      this.add(new RowFilter.CustomExpression(metadata, targetIndex, value));
   }

   private void add(RowFilter.Expression expression) {
      expression.validate();
      this.expressions.add(expression);
   }

   public void addUserExpression(RowFilter.UserExpression e) {
      this.expressions.add(e);
   }

   public List<RowFilter.Expression> getExpressions() {
      return this.expressions;
   }

   public boolean hasExpressionOnClusteringOrRegularColumns() {
      Iterator var1 = this.expressions.iterator();

      ColumnMetadata column;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         RowFilter.Expression expression = (RowFilter.Expression)var1.next();
         column = expression.column();
      } while(!column.isClusteringColumn() && !column.isRegular());

      return true;
   }

   public abstract Flow<FlowableUnfilteredPartition> filter(Flow<FlowableUnfilteredPartition> var1, TableMetadata var2, int var3);

   public Flow<Boolean> isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, int nowInSec) {
      Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
      return purged == null?Flow.just(Boolean.valueOf(this.expressions.isEmpty())):Flow.fromIterable(this.expressions).flatMap((e) -> {
         return e.isSatisfiedBy(metadata, partitionKey, purged);
      }).takeWhile((satisfied) -> {
         return satisfied.booleanValue();
      }).reduce(Integer.valueOf(0), (val, satisfied) -> {
         return Integer.valueOf(val.intValue() + 1);
      }).map((val) -> {
         return Boolean.valueOf(val.intValue() == this.expressions.size());
      });
   }

   public boolean partitionKeyRestrictionsAreSatisfiedBy(DecoratedKey key, AbstractType<?> keyValidator) {
      Iterator var3 = this.expressions.iterator();

      while(var3.hasNext()) {
         RowFilter.Expression e = (RowFilter.Expression)var3.next();
         if(e.column.isPartitionKey()) {
            ByteBuffer value = keyValidator instanceof CompositeType?((CompositeType)keyValidator).split(key.getKey())[e.column.position()]:key.getKey();
            if(!e.operator().isSatisfiedBy(e.column.type, value, e.value)) {
               return false;
            }
         }
      }

      return true;
   }

   public boolean clusteringKeyRestrictionsAreSatisfiedBy(Clustering clustering) {
      Iterator var2 = this.expressions.iterator();

      RowFilter.Expression e;
      do {
         if(!var2.hasNext()) {
            return true;
         }

         e = (RowFilter.Expression)var2.next();
      } while(!e.column.isClusteringColumn() || e.operator().isSatisfiedBy(e.column.type, clustering.get(e.column.position()), e.value));

      return false;
   }

   public RowFilter without(RowFilter.Expression expression) {
      assert this.expressions.contains(expression);

      if(this.expressions.size() == 1) {
         return NONE;
      } else {
         List<RowFilter.Expression> newExpressions = new ArrayList(this.expressions.size() - 1);
         Iterator var3 = this.expressions.iterator();

         while(var3.hasNext()) {
            RowFilter.Expression e = (RowFilter.Expression)var3.next();
            if(!e.equals(expression)) {
               newExpressions.add(e);
            }
         }

         return this.withNewExpressions(newExpressions);
      }
   }

   public RowFilter withoutExpressions() {
      return this.withNewExpressions(Collections.emptyList());
   }

   protected abstract RowFilter withNewExpressions(List<RowFilter.Expression> var1);

   public boolean isEmpty() {
      return this.expressions.isEmpty();
   }

   public Iterator<RowFilter.Expression> iterator() {
      return this.expressions.iterator();
   }

   public boolean equals(Object other) {
      if(!(other instanceof RowFilter)) {
         return false;
      } else {
         RowFilter that = (RowFilter)other;
         return this.expressions.equals(that.expressions);
      }
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();

      for(int i = 0; i < this.expressions.size(); ++i) {
         if(i > 0) {
            sb.append(" AND ");
         }

         sb.append(this.expressions.get(i));
      }

      return sb.toString();
   }

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(RowFilter filter, DataOutputPlus out) throws IOException {
         out.writeBoolean(false);
         out.writeUnsignedVInt((long)filter.expressions.size());
         Iterator var3 = filter.expressions.iterator();

         while(var3.hasNext()) {
            RowFilter.Expression expr = (RowFilter.Expression)var3.next();
            ((RowFilter.Expression.Serializer)RowFilter.Expression.serializers.get(this.version)).serialize(expr, out);
         }

      }

      public RowFilter deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         in.readBoolean();
         int size = (int)in.readUnsignedVInt();
         List<RowFilter.Expression> expressions = new ArrayList(size);

         for(int i = 0; i < size; ++i) {
            expressions.add(((RowFilter.Expression.Serializer)RowFilter.Expression.serializers.get(this.version)).deserialize(in, metadata));
         }

         return new RowFilter.CQLFilter(expressions, null);
      }

      public long serializedSize(RowFilter filter) {
         long size = (long)(1 + TypeSizes.sizeofUnsignedVInt((long)filter.expressions.size()));

         RowFilter.Expression expr;
         for(Iterator var4 = filter.expressions.iterator(); var4.hasNext(); size += ((RowFilter.Expression.Serializer)RowFilter.Expression.serializers.get(this.version)).serializedSize(expr)) {
            expr = (RowFilter.Expression)var4.next();
         }

         return size;
      }
   }

   public abstract static class UserExpression extends RowFilter.Expression {
      private static final RowFilter.UserExpression.DeserializerRegistry deserializers = new RowFilter.UserExpression.DeserializerRegistry(null);

      public static void register(Class<? extends RowFilter.UserExpression> expressionClass, RowFilter.UserExpression.Deserializer deserializer) {
         deserializers.registerUserExpressionClass(expressionClass, deserializer);
      }

      private static RowFilter.UserExpression deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         int id = in.readInt();
         RowFilter.UserExpression.Deserializer deserializer = deserializers.getDeserializer(id);

         assert deserializer != null : "No user defined expression type registered with id " + id;

         return deserializer.deserialize(in, version, metadata);
      }

      private static void serialize(RowFilter.UserExpression expression, DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
         Integer id = deserializers.getId(expression);

         assert id != null : "User defined expression type " + expression.getClass().getName() + " is not registered";

         out.writeInt(id.intValue());
         expression.serialize(out, version);
      }

      private static long serializedSize(RowFilter.UserExpression expression, ReadVerbs.ReadVersion version) {
         return 4L + expression.serializedSize(version);
      }

      protected UserExpression(ColumnMetadata column, Operator operator, ByteBuffer value) {
         super(column, operator, value);
      }

      public RowFilter.Expression.Kind kind() {
         return RowFilter.Expression.Kind.USER;
      }

      protected abstract void serialize(DataOutputPlus var1, ReadVerbs.ReadVersion var2) throws IOException;

      protected abstract long serializedSize(ReadVerbs.ReadVersion var1);

      protected abstract static class Deserializer {
         protected Deserializer() {
         }

         protected abstract RowFilter.UserExpression deserialize(DataInputPlus var1, ReadVerbs.ReadVersion var2, TableMetadata var3) throws IOException;
      }

      private static final class DeserializerRegistry {
         private final AtomicInteger counter;
         private final ConcurrentMap<Integer, RowFilter.UserExpression.Deserializer> deserializers;
         private final ConcurrentMap<Class<? extends RowFilter.UserExpression>, Integer> registeredClasses;

         private DeserializerRegistry() {
            this.counter = new AtomicInteger(0);
            this.deserializers = new ConcurrentHashMap();
            this.registeredClasses = new ConcurrentHashMap();
         }

         public void registerUserExpressionClass(Class<? extends RowFilter.UserExpression> expressionClass, RowFilter.UserExpression.Deserializer deserializer) {
            int id = ((Integer)this.registeredClasses.computeIfAbsent(expressionClass, (cls) -> {
               return Integer.valueOf(this.counter.getAndIncrement());
            })).intValue();
            this.deserializers.put(Integer.valueOf(id), deserializer);
            RowFilter.logger.debug("Registered user defined expression type {} and serializer {} with identifier {}", new Object[]{expressionClass.getName(), deserializer.getClass().getName(), Integer.valueOf(id)});
         }

         public Integer getId(RowFilter.UserExpression expression) {
            return (Integer)this.registeredClasses.get(expression.getClass());
         }

         public RowFilter.UserExpression.Deserializer getDeserializer(int id) {
            return (RowFilter.UserExpression.Deserializer)this.deserializers.get(Integer.valueOf(id));
         }
      }
   }

   public static final class CustomExpression extends RowFilter.Expression {
      private final IndexMetadata targetIndex;
      private final TableMetadata table;

      public CustomExpression(TableMetadata table, IndexMetadata targetIndex, ByteBuffer value) {
         super(makeDefinition(table, targetIndex), Operator.EQ, value);
         this.targetIndex = targetIndex;
         this.table = table;
      }

      private static ColumnMetadata makeDefinition(TableMetadata table, IndexMetadata index) {
         return ColumnMetadata.regularColumn(table, ByteBuffer.wrap(index.name.getBytes()), BytesType.instance);
      }

      public IndexMetadata getTargetIndex() {
         return this.targetIndex;
      }

      public ByteBuffer getValue() {
         return this.value;
      }

      public String toString() {
         return String.format("expr(%s, %s)", new Object[]{this.targetIndex.name, Keyspace.openAndGetStore(this.table).indexManager.getIndex(this.targetIndex).customExpressionValueType()});
      }

      public RowFilter.Expression.Kind kind() {
         return RowFilter.Expression.Kind.CUSTOM;
      }

      public Flow<Boolean> isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
         return Flow.just(Boolean.valueOf(true));
      }
   }

   private static class MapEqualityExpression extends RowFilter.Expression {
      private final ByteBuffer key;

      public MapEqualityExpression(ColumnMetadata column, ByteBuffer key, Operator operator, ByteBuffer value) {
         super(column, operator, value);

         assert column.type instanceof MapType && operator == Operator.EQ;

         this.key = key;
      }

      public void validate() throws InvalidRequestException {
         RequestValidations.checkNotNull(this.key, "Unsupported null map key for column %s", this.column.name);
         RequestValidations.checkBindValueSet(this.key, "Unsupported unset map key for column %s", this.column.name);
         RequestValidations.checkNotNull(this.value, "Unsupported null map value for column %s", this.column.name);
         RequestValidations.checkBindValueSet(this.value, "Unsupported unset map value for column %s", this.column.name);
      }

      public ByteBuffer getIndexValue() {
         return CompositeType.build(new ByteBuffer[]{this.key, this.value});
      }

      public Flow<Boolean> isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
         return Flow.just(Boolean.valueOf(this.isSatisfiedByInternal(metadata, partitionKey, row)));
      }

      private boolean isSatisfiedByInternal(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
         assert this.key != null;

         assert this.value != null;

         if(row.isStatic() != this.column.isStatic()) {
            return true;
         } else {
            MapType<?, ?> mt = (MapType)this.column.type;
            if(this.column.isComplex()) {
               Cell cell = row.getCell(this.column, CellPath.create(this.key));
               return cell != null && mt.valueComparator().compare(cell.value(), this.value) == 0;
            } else {
               ByteBuffer serializedMap = this.getValue(metadata, partitionKey, row);
               if(serializedMap == null) {
                  return false;
               } else {
                  ByteBuffer foundValue = mt.getSerializer().getSerializedValue(serializedMap, this.key, mt.getKeysType());
                  return foundValue != null && mt.valueComparator().compare(foundValue, this.value) == 0;
               }
            }
         }
      }

      public String toString() {
         MapType<?, ?> mt = (MapType)this.column.type;
         return String.format("%s[%s] = %s", new Object[]{this.column.name, mt.nameComparator().getString(this.key), mt.valueComparator().getString(this.value)});
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(!(o instanceof RowFilter.MapEqualityExpression)) {
            return false;
         } else {
            RowFilter.MapEqualityExpression that = (RowFilter.MapEqualityExpression)o;
            return Objects.equals(this.column.name, that.column.name) && Objects.equals(this.operator, that.operator) && Objects.equals(this.key, that.key) && Objects.equals(this.value, that.value);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.column.name, this.operator, this.key, this.value});
      }

      public RowFilter.Expression.Kind kind() {
         return RowFilter.Expression.Kind.MAP_EQUALITY;
      }
   }

   public static class SimpleExpression extends RowFilter.Expression {
      SimpleExpression(ColumnMetadata column, Operator operator, ByteBuffer value) {
         super(column, operator, value);
      }

      public Flow<Boolean> isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
         return Flow.just(Boolean.valueOf(this.isSatisfiedByInternal(metadata, partitionKey, row)));
      }

      private boolean isSatisfiedByInternal(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
         assert this.value != null;

         ByteBuffer foundValue;
         ByteBuffer foundValue;
         switch(null.$SwitchMap$org$apache$cassandra$cql3$Operator[this.operator.ordinal()]) {
         case 1:
         case 2:
         case 3:
         case 4:
         case 5:
            assert !this.column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";

            if(this.column.type.isCounter()) {
               foundValue = this.getValue(metadata, partitionKey, row);
               if(foundValue == null) {
                  return false;
               } else {
                  foundValue = LongType.instance.decompose(Long.valueOf(CounterContext.instance().total(foundValue)));
                  return this.operator.isSatisfiedBy(LongType.instance, foundValue, this.value);
               }
            } else {
               foundValue = this.getValue(metadata, partitionKey, row);
               return foundValue != null && this.operator.isSatisfiedBy(this.column.type, foundValue, this.value);
            }
         case 6:
         case 7:
         case 8:
         case 9:
         case 10:
            assert !this.column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";

            foundValue = this.getValue(metadata, partitionKey, row);
            return foundValue != null && this.operator.isSatisfiedBy(this.column.type, foundValue, this.value);
         case 11:
            assert this.column.type.isCollection();

            CollectionType<?> type = (CollectionType)this.column.type;
            if(!this.column.isComplex()) {
               foundValue = this.getValue(metadata, partitionKey, row);
               if(foundValue == null) {
                  return false;
               } else {
                  switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[type.kind.ordinal()]) {
                  case 1:
                     ListType<?> listType = (ListType)type;
                     return ((List)listType.compose(foundValue)).contains(listType.getElementsType().compose(this.value));
                  case 2:
                     SetType<?> setType = (SetType)type;
                     return ((Set)setType.compose(foundValue)).contains(setType.getElementsType().compose(this.value));
                  case 3:
                     MapType<?, ?> mapType = (MapType)type;
                     return ((Map)mapType.compose(foundValue)).containsValue(mapType.getValuesType().compose(this.value));
                  default:
                     throw new AssertionError();
                  }
               }
            } else {
               ComplexColumnData complexData = row.getComplexColumnData(this.column);
               if(complexData != null) {
                  Iterator var12 = complexData.iterator();

                  while(var12.hasNext()) {
                     Cell cell = (Cell)var12.next();
                     if(type.kind == CollectionType.Kind.SET) {
                        if(type.nameComparator().compare(cell.path().get(0), this.value) == 0) {
                           return true;
                        }
                     } else if(type.valueComparator().compare(cell.value(), this.value) == 0) {
                        return true;
                     }
                  }
               }

               return false;
            }
         case 12:
            if($assertionsDisabled || this.column.type.isCollection() && this.column.type instanceof MapType) {
               MapType<?, ?> mapType = (MapType)this.column.type;
               if(this.column.isComplex()) {
                  return row.getCell(this.column, CellPath.create(this.value)) != null;
               }

               ByteBuffer foundValue = this.getValue(metadata, partitionKey, row);
               return foundValue != null && mapType.getSerializer().getSerializedValue(foundValue, this.value, mapType.getKeysType()) != null;
            }

            throw new AssertionError();
         case 13:
            throw new AssertionError();
         default:
            throw new AssertionError();
         }
      }

      public String toString() {
         AbstractType<?> type = this.column.type;
         switch(null.$SwitchMap$org$apache$cassandra$cql3$Operator[this.operator.ordinal()]) {
         case 11:
            assert type instanceof CollectionType;

            CollectionType<?> ct = (CollectionType)type;
            type = ct.kind == CollectionType.Kind.SET?ct.nameComparator():ct.valueComparator();
            break;
         case 12:
            assert type instanceof MapType;

            type = ((MapType)type).nameComparator();
            break;
         case 13:
            type = ListType.getInstance((AbstractType)type, false);
         }

         return String.format("%s %s %s", new Object[]{this.column.name, this.operator, ((AbstractType)type).getString(this.value)});
      }

      public RowFilter.Expression.Kind kind() {
         return RowFilter.Expression.Kind.SIMPLE;
      }
   }

   public abstract static class Expression {
      private static final Versioned<ReadVerbs.ReadVersion, RowFilter.Expression.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
         return new RowFilter.Expression.Serializer(x$0, null);
      });
      protected final ColumnMetadata column;
      protected final Operator operator;
      protected final ByteBuffer value;

      public abstract RowFilter.Expression.Kind kind();

      protected Expression(ColumnMetadata column, Operator operator, ByteBuffer value) {
         this.column = column;
         this.operator = operator;
         this.value = value;
      }

      public boolean isCustom() {
         return this.kind() == RowFilter.Expression.Kind.CUSTOM;
      }

      public boolean isUserDefined() {
         return this.kind() == RowFilter.Expression.Kind.USER;
      }

      public ColumnMetadata column() {
         return this.column;
      }

      public Operator operator() {
         return this.operator;
      }

      public boolean isContains() {
         return Operator.CONTAINS == this.operator;
      }

      public boolean isContainsKey() {
         return Operator.CONTAINS_KEY == this.operator;
      }

      public ByteBuffer getIndexValue() {
         return this.value;
      }

      public void validate() {
         RequestValidations.checkNotNull(this.value, "Unsupported null value for column %s", this.column.name);
         RequestValidations.checkBindValueSet(this.value, "Unsupported unset value for column %s", this.column.name);
      }

      /** @deprecated */
      @Deprecated
      public void validateForIndexing() {
         RequestValidations.checkFalse(this.value.remaining() > '\uffff', "Index expression values may not be larger than 64K");
      }

      public abstract Flow<Boolean> isSatisfiedBy(TableMetadata var1, DecoratedKey var2, Row var3);

      protected ByteBuffer getValue(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
         switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[this.column.kind.ordinal()]) {
         case 1:
            return metadata.partitionKeyType instanceof CompositeType?CompositeType.extractComponent(partitionKey.getKey(), this.column.position()):partitionKey.getKey();
         case 2:
            return row.clustering().get(this.column.position());
         default:
            Cell cell = row.getCell(this.column);
            return cell == null?null:cell.value();
         }
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(!(o instanceof RowFilter.Expression)) {
            return false;
         } else {
            RowFilter.Expression that = (RowFilter.Expression)o;
            return Objects.equals(this.kind(), that.kind()) && Objects.equals(this.column.name, that.column.name) && Objects.equals(this.operator, that.operator) && Objects.equals(this.value, that.value);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.column.name, this.operator, this.value});
      }

      private static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
         private Serializer(ReadVerbs.ReadVersion version) {
            super(version);
         }

         public void serialize(RowFilter.Expression expression, DataOutputPlus out) throws IOException {
            out.writeByte(expression.kind().ordinal());
            if(expression.kind() == RowFilter.Expression.Kind.CUSTOM) {
               IndexMetadata.serializer.serialize(((RowFilter.CustomExpression)expression).targetIndex, out);
               ByteBufferUtil.writeWithShortLength(expression.value, out);
            } else if(expression.kind() == RowFilter.Expression.Kind.USER) {
               RowFilter.UserExpression.serialize((RowFilter.UserExpression)expression, out, (ReadVerbs.ReadVersion)this.version);
            } else {
               ByteBufferUtil.writeWithShortLength(expression.column.name.bytes, out);
               expression.operator.writeTo(out);
               switch(null.$SwitchMap$org$apache$cassandra$db$filter$RowFilter$Expression$Kind[expression.kind().ordinal()]) {
               case 1:
                  ByteBufferUtil.writeWithShortLength(((RowFilter.SimpleExpression)expression).value, out);
                  break;
               case 2:
                  RowFilter.MapEqualityExpression mexpr = (RowFilter.MapEqualityExpression)expression;
                  ByteBufferUtil.writeWithShortLength(mexpr.key, out);
                  ByteBufferUtil.writeWithShortLength(mexpr.value, out);
               }

            }
         }

         public RowFilter.Expression deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
            RowFilter.Expression.Kind kind = RowFilter.Expression.Kind.values()[in.readByte()];
            if(kind == RowFilter.Expression.Kind.CUSTOM) {
               return new RowFilter.CustomExpression(metadata, IndexMetadata.serializer.deserialize(in, metadata), ByteBufferUtil.readWithShortLength(in));
            } else if(kind == RowFilter.Expression.Kind.USER) {
               return RowFilter.UserExpression.deserialize(in, (ReadVerbs.ReadVersion)this.version, metadata);
            } else {
               ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
               Operator operator = Operator.readFrom(in);
               ColumnMetadata column = metadata.getColumn(name);
               if(!metadata.isCompactTable() && column == null) {
                  throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");
               } else {
                  switch(null.$SwitchMap$org$apache$cassandra$db$filter$RowFilter$Expression$Kind[kind.ordinal()]) {
                  case 1:
                     return new RowFilter.SimpleExpression(column, operator, ByteBufferUtil.readWithShortLength(in));
                  case 2:
                     ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                     ByteBuffer value = ByteBufferUtil.readWithShortLength(in);
                     return new RowFilter.MapEqualityExpression(column, key, operator, value);
                  default:
                     throw new AssertionError();
                  }
               }
            }
         }

         public long serializedSize(RowFilter.Expression expression) {
            long size = 1L;
            if(expression.kind() != RowFilter.Expression.Kind.CUSTOM && expression.kind() != RowFilter.Expression.Kind.USER) {
               size += (long)(ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes) + expression.operator.serializedSize());
            }

            switch(null.$SwitchMap$org$apache$cassandra$db$filter$RowFilter$Expression$Kind[expression.kind().ordinal()]) {
            case 1:
               size += (long)ByteBufferUtil.serializedSizeWithShortLength(((RowFilter.SimpleExpression)expression).value);
               break;
            case 2:
               RowFilter.MapEqualityExpression mexpr = (RowFilter.MapEqualityExpression)expression;
               size += (long)(ByteBufferUtil.serializedSizeWithShortLength(mexpr.key) + ByteBufferUtil.serializedSizeWithShortLength(mexpr.value));
               break;
            case 3:
               size += IndexMetadata.serializer.serializedSize(((RowFilter.CustomExpression)expression).targetIndex) + (long)ByteBufferUtil.serializedSizeWithShortLength(expression.value);
               break;
            case 4:
               size += RowFilter.UserExpression.serializedSize((RowFilter.UserExpression)expression, (ReadVerbs.ReadVersion)this.version);
            }

            return size;
         }
      }

      public static enum Kind {
         SIMPLE,
         MAP_EQUALITY,
         UNUSED1,
         CUSTOM,
         USER;

         private Kind() {
         }
      }
   }

   private static class CQLFilter extends RowFilter {
      private CQLFilter(List<RowFilter.Expression> expressions) {
         super(expressions);
      }

      public Flow<FlowableUnfilteredPartition> filter(Flow<FlowableUnfilteredPartition> iter, TableMetadata metadata, int nowInSec) {
         if(this.expressions.isEmpty()) {
            return iter;
         } else {
            List<RowFilter.Expression> partitionLevelExpressions = new ArrayList();
            List<RowFilter.Expression> rowLevelExpressions = new ArrayList();
            Iterator var6 = this.expressions.iterator();

            while(true) {
               while(var6.hasNext()) {
                  RowFilter.Expression e = (RowFilter.Expression)var6.next();
                  if(!e.column.isStatic() && !e.column.isPartitionKey()) {
                     rowLevelExpressions.add(e);
                  } else {
                     partitionLevelExpressions.add(e);
                  }
               }

               long numberOfRegularColumnExpressions = (long)rowLevelExpressions.size();
               boolean filterNonStaticColumns = numberOfRegularColumnExpressions > 0L;
               return iter.flatMap((partition) -> {
                  DecoratedKey pk = partition.partitionKey();
                  return Flow.fromIterable(partitionLevelExpressions).flatMap((e) -> {
                     return e.isSatisfiedBy(metadata, pk, partition.staticRow());
                  }).takeWhile((satisfied) -> {
                     return satisfied.booleanValue();
                  }).reduce(Integer.valueOf(0), (val, satisfied) -> {
                     return Integer.valueOf(val.intValue() + 1);
                  }).map((val) -> {
                     return Boolean.valueOf(val.intValue() == partitionLevelExpressions.size());
                  }).skippingMap((allSatisfied) -> {
                     return allSatisfied.booleanValue()?partition:null;
                  });
               }).flatMap((p) -> {
                  DecoratedKey pk = p.partitionKey();
                  Flow<Unfiltered> content = p.content().flatMap((unfiltered) -> {
                     if(unfiltered.isRow()) {
                        Row purged = ((Row)unfiltered).purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
                        return purged == null?Flow.empty():Flow.fromIterable(rowLevelExpressions).flatMap((e) -> {
                           return e.isSatisfiedBy(metadata, pk, purged);
                        }).takeWhile((satisfied) -> {
                           return satisfied.booleanValue();
                        }).reduce(Integer.valueOf(0), (val, satisfied) -> {
                           return Integer.valueOf(val.intValue() + 1);
                        }).map((val) -> {
                           return Boolean.valueOf(val.intValue() == rowLevelExpressions.size());
                        }).skippingMap((allSatisfied) -> {
                           return allSatisfied.booleanValue()?unfiltered:null;
                        });
                     } else {
                        return Flow.just(unfiltered);
                     }
                  });
                  return filterNonStaticColumns?content.skipMapEmpty((c) -> {
                     return FlowableUnfilteredPartition.create(p.header(), p.staticRow(), c);
                  }):Flow.just(FlowableUnfilteredPartition.create(p.header(), p.staticRow(), content));
               });
            }
         }
      }

      protected RowFilter withNewExpressions(List<RowFilter.Expression> expressions) {
         return new RowFilter.CQLFilter(expressions);
      }
   }
}
