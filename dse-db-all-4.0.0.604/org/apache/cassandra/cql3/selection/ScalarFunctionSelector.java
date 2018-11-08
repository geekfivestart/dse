package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.transport.ProtocolVersion;

final class ScalarFunctionSelector extends AbstractFunctionSelector<ScalarFunction> {
   protected static final Selector.SelectorDeserializer deserializer = new AbstractFunctionSelector.AbstractFunctionSelectorDeserializer() {
      protected Selector newFunctionSelector(ProtocolVersion version, Function function, List<Selector> argSelectors) {
         return new ScalarFunctionSelector(version, function, argSelectors);
      }
   };

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      int i = 0;

      for(int m = this.argSelectors.size(); i < m; ++i) {
         Selector s = (Selector)this.argSelectors.get(i);
         s.addInput(protocolVersion, input);
      }

   }

   public void reset() {
   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      int i = 0;

      for(int m = this.argSelectors.size(); i < m; ++i) {
         Selector s = (Selector)this.argSelectors.get(i);
         this.setArg(i, s.getOutput(protocolVersion));
         s.reset();
      }

      return ((ScalarFunction)this.fun).execute(this.args());
   }

   public void validateForGroupBy() {
      RequestValidations.checkTrue(((ScalarFunction)this.fun).isNative() || !DatabaseDescriptor.enableUserDefinedFunctionsThreads(), "User defined functions are not supported in the GROUP BY clause when asynchronous UDF execution is enabled. Asynchronous UDF execution can be disabled by setting the configuration property 'enable_user_defined_functions_threads' to false in cassandra.yaml, with the security risks described in the yaml file.");
      RequestValidations.checkTrue(((ScalarFunction)this.fun).isMonotonic(), "Only monotonic functions are supported in the GROUP BY clause. Got: %s ", this.fun);
      int i = 0;

      for(int m = this.argSelectors.size(); i < m; ++i) {
         ((Selector)this.argSelectors.get(i)).validateForGroupBy();
      }

   }

   ScalarFunctionSelector(ProtocolVersion version, Function fun, List<Selector> argSelectors) {
      super(Selector.Kind.SCALAR_FUNCTION_SELECTOR, version, (ScalarFunction)fun, argSelectors);
   }
}
