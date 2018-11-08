package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public interface Term {
   void collectMarkerSpecification(VariableSpecifications var1);

   Term.Terminal bind(QueryOptions var1) throws InvalidRequestException;

   ByteBuffer bindAndGet(QueryOptions var1) throws InvalidRequestException;

   boolean containsBindMarker();

   default boolean isTerminal() {
      return false;
   }

   void addFunctionsTo(List<Function> var1);

   public abstract static class NonTerminal implements Term {
      public NonTerminal() {
      }

      public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException {
         Term.Terminal t = this.bind(options);
         return t == null?null:t.get(options.getProtocolVersion());
      }
   }

   public abstract static class MultiItemTerminal extends Term.Terminal {
      public MultiItemTerminal() {
      }

      public abstract List<ByteBuffer> getElements();
   }

   public abstract static class Terminal implements Term {
      public Terminal() {
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
      }

      public Term.Terminal bind(QueryOptions options) {
         return this;
      }

      public void addFunctionsTo(List<Function> functions) {
      }

      public boolean containsBindMarker() {
         return false;
      }

      public boolean isTerminal() {
         return true;
      }

      public abstract ByteBuffer get(ProtocolVersion var1) throws InvalidRequestException;

      public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException {
         return this.get(options.getProtocolVersion());
      }
   }

   public abstract static class MultiColumnRaw extends Term.Raw {
      public MultiColumnRaw() {
      }

      public abstract Term prepare(String var1, List<? extends ColumnSpecification> var2) throws InvalidRequestException;
   }

   public abstract static class Raw implements AssignmentTestable {
      public Raw() {
      }

      public abstract Term prepare(String var1, ColumnSpecification var2) throws InvalidRequestException;

      public abstract String getText();

      public abstract AbstractType<?> getExactTypeIfKnown(String var1);

      public String toString() {
         return this.getText();
      }
   }
}
