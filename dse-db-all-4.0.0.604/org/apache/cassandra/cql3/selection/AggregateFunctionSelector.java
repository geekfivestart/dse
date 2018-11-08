package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.functions.AggregateFunction;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.transport.ProtocolVersion;

final class AggregateFunctionSelector extends AbstractFunctionSelector<AggregateFunction> {
   protected static final Selector.SelectorDeserializer deserializer = new AbstractFunctionSelector.AbstractFunctionSelectorDeserializer() {
      protected Selector newFunctionSelector(ProtocolVersion version, Function function, List<Selector> argSelectors) {
         return new AggregateFunctionSelector(version, function, argSelectors);
      }
   };
   private final AggregateFunction.Aggregate aggregate;

   public boolean isAggregate() {
      return true;
   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      int i = 0;

      for(int m = this.argSelectors.size(); i < m; ++i) {
         Selector s = (Selector)this.argSelectors.get(i);
         s.addInput(protocolVersion, input);
         this.setArg(i, s.getOutput(protocolVersion));
         s.reset();
      }

      this.aggregate.addInput(this.args());
   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      return this.aggregate.compute(protocolVersion);
   }

   public void reset() {
      this.aggregate.reset();
   }

   AggregateFunctionSelector(ProtocolVersion version, Function fun, List<Selector> argSelectors) {
      super(Selector.Kind.AGGREGATE_FUNCTION_SELECTOR, version, (AggregateFunction)fun, argSelectors);
      this.aggregate = ((AggregateFunction)this.fun).newAggregate();
   }
}
