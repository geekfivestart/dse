package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.github.jamm.Unmetered;

@Unmetered
public interface Function extends AssignmentTestable {
   ByteBuffer UNRESOLVED = ByteBuffer.allocate(0);

   FunctionName name();

   List<AbstractType<?>> argTypes();

   AbstractType<?> returnType();

   boolean isNative();

   boolean isDeterministic();

   boolean isAggregate();

   void addFunctionsTo(List<Function> var1);

   boolean hasReferenceTo(Function var1);

   String columnName(List<String> var1);

   Arguments newArguments(ProtocolVersion var1);
}
