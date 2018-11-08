package org.apache.cassandra.cql3.selection;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.PartialScalarFunction;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.text.StrBuilder;

abstract class AbstractFunctionSelector<T extends Function> extends Selector {
   protected final T fun;
   private final Arguments args;
   protected final List<Selector> argSelectors;

   public static Selector.Factory newFactory(final Function fun, final SelectorFactories factories) throws InvalidRequestException {
      if(fun.isAggregate() && factories.doesAggregation()) {
         throw new InvalidRequestException("aggregate functions cannot be used as arguments of aggregate functions");
      } else {
         return new Selector.Factory() {
            protected String getColumnName() {
               return fun.columnName(factories.getColumnNames());
            }

            protected AbstractType<?> getReturnType() {
               return fun.returnType();
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
               SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
               Iterator var4 = factories.iterator();

               while(var4.hasNext()) {
                  Selector.Factory factory = (Selector.Factory)var4.next();
                  factory.addColumnMapping(tmpMapping, resultsColumn);
               }

               if(tmpMapping.getMappings().get(resultsColumn).isEmpty()) {
                  mapping.addMapping(resultsColumn, (ColumnMetadata)null);
               } else {
                  mapping.addMapping(resultsColumn, (Iterable)tmpMapping.getMappings().values());
               }

            }

            public void addFunctionsTo(List<Function> functions) {
               fun.addFunctionsTo(functions);
               factories.addFunctionsTo(functions);
            }

            public Selector newInstance(QueryOptions options) throws InvalidRequestException {
               return (Selector)(fun.isAggregate()?new AggregateFunctionSelector(options.getProtocolVersion(), fun, factories.newInstances(options)):this.createScalarSelector(options, (ScalarFunction)fun, factories.newInstances(options)));
            }

            public boolean isWritetimeSelectorFactory() {
               return factories.containsWritetimeSelectorFactory();
            }

            public boolean isTTLSelectorFactory() {
               return factories.containsTTLSelectorFactory();
            }

            public boolean isAggregateSelectorFactory() {
               return fun.isAggregate() || factories.doesAggregation();
            }

            private Selector createScalarSelector(QueryOptions options, ScalarFunction function, List<Selector> argSelectors) {
               ProtocolVersion version = options.getProtocolVersion();
               int terminalCount = 0;
               List<ByteBuffer> terminalArgs = new ArrayList(argSelectors.size());
               Iterator var7 = argSelectors.iterator();

               while(var7.hasNext()) {
                  Selector selector = (Selector)var7.next();
                  if(selector.isTerminal()) {
                     ++terminalCount;
                     ByteBuffer output = selector.getOutput(version);
                     RequestValidations.checkBindValueSet(output, "Invalid unset value for argument in call to function %s", fun.name().name);
                     terminalArgs.add(output);
                  } else {
                     terminalArgs.add(Function.UNRESOLVED);
                  }
               }

               if(terminalCount == 0) {
                  return new ScalarFunctionSelector(version, fun, argSelectors);
               } else {
                  ScalarFunction partialFunction = function.partialApplication(version, terminalArgs);
                  if(terminalCount == argSelectors.size() && fun.isDeterministic()) {
                     Arguments arguments = partialFunction.newArguments(version);
                     return new TermSelector(partialFunction.execute(arguments), partialFunction.returnType());
                  } else {
                     List<Selector> remainingSelectors = new ArrayList(argSelectors.size() - terminalCount);
                     Iterator var14 = argSelectors.iterator();

                     while(var14.hasNext()) {
                        Selector selectorx = (Selector)var14.next();
                        if(!selectorx.isTerminal()) {
                           remainingSelectors.add(selectorx);
                        }
                     }

                     return new ScalarFunctionSelector(version, partialFunction, remainingSelectors);
                  }
               }
            }

            public boolean areAllFetchedColumnsKnown() {
               return Iterables.all(factories, (f) -> {
                  return f.areAllFetchedColumnsKnown();
               });
            }

            public void addFetchedColumns(ColumnFilter.Builder builder) {
               Iterator var2 = factories.iterator();

               while(var2.hasNext()) {
                  Selector.Factory factory = (Selector.Factory)var2.next();
                  factory.addFetchedColumns(builder);
               }

            }
         };
      }
   }

   protected AbstractFunctionSelector(Selector.Kind kind, ProtocolVersion version, T fun, List<Selector> argSelectors) {
      super(kind);
      this.fun = fun;
      this.argSelectors = argSelectors;
      this.args = fun.newArguments(version);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof AbstractFunctionSelector)) {
         return false;
      } else {
         AbstractFunctionSelector<?> s = (AbstractFunctionSelector)o;
         return Objects.equals(this.fun.name(), s.fun.name()) && Objects.equals(this.fun.argTypes(), s.fun.argTypes()) && Objects.equals(this.argSelectors, s.argSelectors);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.fun.name(), this.fun.argTypes(), this.argSelectors});
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      Iterator var2 = this.argSelectors.iterator();

      while(var2.hasNext()) {
         Selector selector = (Selector)var2.next();
         selector.addFetchedColumns(builder);
      }

   }

   protected void setArg(int i, ByteBuffer value) throws InvalidRequestException {
      RequestValidations.checkBindValueSet(value, "Invalid unset value for argument in call to function %s", this.fun.name().name);
      this.args.set(i, value);
   }

   protected Arguments args() {
      return this.args;
   }

   public AbstractType<?> getType() {
      return this.fun.returnType();
   }

   public String toString() {
      return (new StrBuilder()).append(this.fun.name()).append("(").appendWithSeparators(this.argSelectors, ", ").append(")").toString();
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      boolean isPartial = this.fun instanceof PartialScalarFunction;
      Function function = isPartial?((PartialScalarFunction)this.fun).getFunction():this.fun;
      FunctionName name = function.name();
      int size = TypeSizes.sizeof(name.keyspace) + TypeSizes.sizeof(name.name);
      List<AbstractType<?>> argTypes = function.argTypes();
      size += TypeSizes.sizeofUnsignedVInt((long)argTypes.size());
      int numberOfRemainingArguments = 0;

      int i;
      for(i = argTypes.size(); numberOfRemainingArguments < i; ++numberOfRemainingArguments) {
         size += sizeOf((AbstractType)argTypes.get(numberOfRemainingArguments));
      }

      size += TypeSizes.sizeof(isPartial);
      if(isPartial) {
         List<ByteBuffer> partialParameters = ((PartialScalarFunction)this.fun).getPartialArguments();
         size += TypeSizes.sizeofUnsignedVInt((long)this.computeBitSet(partialParameters));
         i = 0;

         for(int m = partialParameters.size(); i < m; ++i) {
            ByteBuffer buffer = (ByteBuffer)partialParameters.get(i);
            if(buffer != Function.UNRESOLVED) {
               size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);
            }
         }
      }

      numberOfRemainingArguments = this.argSelectors.size();
      size += TypeSizes.sizeofUnsignedVInt((long)numberOfRemainingArguments);

      for(i = 0; i < numberOfRemainingArguments; ++i) {
         size += ((Selector.Serializer)serializers.get(version)).serializedSize((Selector)this.argSelectors.get(i));
      }

      return size;
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      boolean isPartial = this.fun instanceof PartialScalarFunction;
      Function function = isPartial?((PartialScalarFunction)this.fun).getFunction():this.fun;
      FunctionName name = function.name();
      out.writeUTF(name.keyspace);
      out.writeUTF(name.name);
      List<AbstractType<?>> argTypes = function.argTypes();
      int numberOfArguments = argTypes.size();
      out.writeUnsignedVInt((long)numberOfArguments);

      int numberOfRemainingArguments;
      for(numberOfRemainingArguments = 0; numberOfRemainingArguments < numberOfArguments; ++numberOfRemainingArguments) {
         writeType(out, (AbstractType)argTypes.get(numberOfRemainingArguments));
      }

      out.writeBoolean(isPartial);
      int i;
      if(isPartial) {
         List<ByteBuffer> partialParameters = ((PartialScalarFunction)this.fun).getPartialArguments();
         out.writeUnsignedVInt((long)this.computeBitSet(partialParameters));
         i = 0;

         for(int m = partialParameters.size(); i < m; ++i) {
            ByteBuffer buffer = (ByteBuffer)partialParameters.get(i);
            if(buffer != Function.UNRESOLVED) {
               ByteBufferUtil.writeWithVIntLength(buffer, out);
            }
         }
      }

      numberOfRemainingArguments = this.argSelectors.size();
      out.writeUnsignedVInt((long)numberOfRemainingArguments);

      for(i = 0; i < numberOfRemainingArguments; ++i) {
         ((Selector.Serializer)serializers.get(version)).serialize((Selector)this.argSelectors.get(i), out);
      }

   }

   private int computeBitSet(List<ByteBuffer> partialParameters) {
      int bitset = 0;
      int i = 0;

      for(int m = partialParameters.size(); i < m; ++i) {
         if(partialParameters.get(i) != Function.UNRESOLVED) {
            bitset |= 1 << i;
         }
      }

      return bitset;
   }

   protected abstract static class AbstractFunctionSelectorDeserializer extends Selector.SelectorDeserializer {
      protected AbstractFunctionSelectorDeserializer() {
      }

      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;
         FunctionName name = new FunctionName(in.readUTF(), in.readUTF());
         KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
         int numberOfArguments = (int)in.readUnsignedVInt();
         List<AbstractType<?>> argTypes = new ArrayList(numberOfArguments);

         for(int i = 0; i < numberOfArguments; ++i) {
            argTypes.add(this.readType(keyspace, in));
         }

         Optional<Function> optional = Schema.instance.findFunction(name, argTypes);
         if(!optional.isPresent()) {
            throw new IOException(String.format("Unknown serialized function %s(%s)", new Object[]{name, argTypes.stream().map((p) -> {
               return p.asCQL3Type().toString();
            }).collect(Collectors.joining(", "))}));
         } else {
            Function function = (Function)optional.get();
            boolean isPartial = in.readBoolean();
            int bitset;
            ArrayList argSelectors;
            int i;
            if(isPartial) {
               bitset = (int)in.readUnsignedVInt();
               argSelectors = new ArrayList(numberOfArguments);

               for(i = 0; i < numberOfArguments; ++i) {
                  boolean isParameterResolved = this.getRightMostBit(bitset) == 1;
                  ByteBuffer parameter = isParameterResolved?ByteBufferUtil.readWithVIntLength(in):Function.UNRESOLVED;
                  argSelectors.add(parameter);
                  bitset >>= 1;
               }

               function = ((ScalarFunction)function).partialApplication(protocolVersion, argSelectors);
            }

            bitset = (int)in.readUnsignedVInt();
            argSelectors = new ArrayList(bitset);

            for(i = 0; i < bitset; ++i) {
               argSelectors.add(((Selector.Serializer)Selector.serializers.get(version)).deserialize(in, metadata));
            }

            return this.newFunctionSelector(protocolVersion, (Function)function, argSelectors);
         }
      }

      private int getRightMostBit(int bitset) {
         return bitset & 1;
      }

      protected abstract Selector newFunctionSelector(ProtocolVersion var1, Function var2, List<Selector> var3);
   }
}
