package org.apache.cassandra.cql3.functions;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JavaDriverUtils;

public final class UDFContextImpl implements UDFContext {
   private final KeyspaceMetadata keyspaceMetadata;
   private final Map<String, UDFDataType> byName = new HashMap();
   private final List<UDFDataType> argTypes;
   private final UDFDataType returnType;

   UDFContextImpl(List<ColumnIdentifier> argNames, List<UDFDataType> argTypes, UDFDataType returnType, KeyspaceMetadata keyspaceMetadata) {
      for(int i = 0; i < argNames.size(); ++i) {
         this.byName.put(((ColumnIdentifier)argNames.get(i)).toString(), argTypes.get(i));
      }

      this.argTypes = argTypes;
      this.returnType = returnType;
      this.keyspaceMetadata = keyspaceMetadata;
   }

   public UDTValue newArgUDTValue(String argName) {
      return newUDTValue(this.getArgumentTypeByName(argName).toDataType());
   }

   public UDTValue newArgUDTValue(int argNum) {
      return newUDTValue(this.getArgumentTypeByIndex(argNum).toDataType());
   }

   public UDTValue newReturnUDTValue() {
      return newUDTValue(this.returnType.toDataType());
   }

   public UDTValue newUDTValue(String udtName) {
      Optional<UserType> udtType = this.keyspaceMetadata.types.get(ByteBufferUtil.bytes(udtName));
      DataType dataType = JavaDriverUtils.driverType((AbstractType)udtType.orElseThrow(() -> {
         return new IllegalArgumentException("No UDT named " + udtName + " in keyspace " + this.keyspaceMetadata.name);
      }));
      return newUDTValue(dataType);
   }

   public TupleValue newArgTupleValue(String argName) {
      return newTupleValue(this.getArgumentTypeByName(argName).toDataType());
   }

   public TupleValue newArgTupleValue(int argNum) {
      return newTupleValue(this.getArgumentTypeByIndex(argNum).toDataType());
   }

   public TupleValue newReturnTupleValue() {
      return newTupleValue(this.returnType.toDataType());
   }

   public TupleValue newTupleValue(String cqlDefinition) {
      AbstractType<?> abstractType = CQLTypeParser.parse(this.keyspaceMetadata.name, cqlDefinition, this.keyspaceMetadata.types);
      DataType dataType = JavaDriverUtils.driverType(abstractType);
      return newTupleValue(dataType);
   }

   private UDFDataType getArgumentTypeByIndex(int index) {
      if(index >= 0 && index < this.argTypes.size()) {
         return (UDFDataType)this.argTypes.get(index);
      } else {
         throw new IllegalArgumentException("Function does not declare an argument with index " + index);
      }
   }

   private UDFDataType getArgumentTypeByName(String argName) {
      UDFDataType arg = (UDFDataType)this.byName.get(argName);
      if(arg == null) {
         throw new IllegalArgumentException("Function does not declare an argument named '" + argName + '\'');
      } else {
         return arg;
      }
   }

   private static UDTValue newUDTValue(DataType dataType) {
      if(!(dataType instanceof com.datastax.driver.core.UserType)) {
         throw new IllegalStateException("Function argument is not a UDT but a " + dataType.getName());
      } else {
         com.datastax.driver.core.UserType userType = (com.datastax.driver.core.UserType)dataType;
         return userType.newValue();
      }
   }

   private static TupleValue newTupleValue(DataType dataType) {
      if(!(dataType instanceof TupleType)) {
         throw new IllegalStateException("Function argument is not a tuple type but a " + dataType.getName());
      } else {
         TupleType tupleType = (TupleType)dataType;
         return tupleType.newValue();
      }
   }
}
