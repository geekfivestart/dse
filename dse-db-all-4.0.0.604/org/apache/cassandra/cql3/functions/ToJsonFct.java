package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ToJsonFct extends NativeScalarFunction {
   public static final FunctionName NAME = FunctionName.nativeFunction("tojson");
   private static final Map<AbstractType<?>, ToJsonFct> instances = new ConcurrentHashMap();

   public static ToJsonFct getInstance(List<AbstractType<?>> argTypes) throws InvalidRequestException {
      if(argTypes.size() != 1) {
         throw new InvalidRequestException(String.format("toJson() only accepts one argument (got %d)", new Object[]{Integer.valueOf(argTypes.size())}));
      } else {
         AbstractType<?> fromType = (AbstractType)argTypes.get(0);
         ToJsonFct func = (ToJsonFct)instances.get(fromType);
         if(func == null) {
            func = new ToJsonFct(fromType);
            instances.put(fromType, func);
         }

         return func;
      }
   }

   private ToJsonFct(AbstractType<?> argType) {
      super("tojson", UTF8Type.instance, new AbstractType[]{argType});
   }

   public Arguments newArguments(ProtocolVersion version) {
      return new FunctionArguments(version, new ArgumentDeserializer[]{new ArgumentDeserializer() {
         public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer) {
            AbstractType<?> argType = (AbstractType)ToJsonFct.this.argTypes.get(0);
            return buffer != null && (buffer.hasRemaining() || !argType.isEmptyValueMeaningless())?((AbstractType)ToJsonFct.this.argTypes.get(0)).toJSONString(buffer, protocolVersion):null;
         }
      }});
   }

   public ByteBuffer execute(Arguments arguments) throws InvalidRequestException {
      assert arguments.size() == 1 : "Expected 1 argument for toJson(), but got " + arguments.size();

      return arguments.containsNulls()?ByteBufferUtil.bytes("null"):ByteBufferUtil.bytes((String)arguments.get(0));
   }
}
