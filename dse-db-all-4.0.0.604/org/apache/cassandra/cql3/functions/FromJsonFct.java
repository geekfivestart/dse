package org.apache.cassandra.cql3.functions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.serializers.MarshalException;

public class FromJsonFct extends NativeScalarFunction {
   public static final FunctionName NAME = FunctionName.nativeFunction("fromjson");
   private static final Map<AbstractType<?>, FromJsonFct> instances = new ConcurrentHashMap();

   public static FromJsonFct getInstance(AbstractType<?> returnType) {
      FromJsonFct func = (FromJsonFct)instances.get(returnType);
      if(func == null) {
         func = new FromJsonFct(returnType);
         instances.put(returnType, func);
      }

      return func;
   }

   private FromJsonFct(AbstractType<?> returnType) {
      super("fromjson", returnType, new AbstractType[]{UTF8Type.instance});
   }

   public ByteBuffer execute(Arguments arguments) {
      assert arguments.size() == 1 : "Unexpectedly got " + arguments.size() + " arguments for fromJson()";

      if(arguments.containsNulls()) {
         return null;
      } else {
         String jsonArg = (String)arguments.get(0);

         try {
            Object object = Json.JSON_OBJECT_MAPPER.readValue(jsonArg, Object.class);
            return object == null?null:this.returnType.fromJSONObject(object).bindAndGet(QueryOptions.forProtocolVersion(arguments.getProtocolVersion()));
         } catch (IOException var4) {
            throw new FunctionExecutionException(NAME, Collections.singletonList("text"), String.format("Could not decode JSON string '%s': %s", new Object[]{jsonArg, var4.toString()}));
         } catch (MarshalException var5) {
            throw FunctionExecutionException.create(this, var5);
         }
      }
   }
}
