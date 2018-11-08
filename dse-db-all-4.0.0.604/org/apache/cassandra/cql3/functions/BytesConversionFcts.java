package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class BytesConversionFcts {
   public static final Function VarcharAsBlobFct;
   public static final Function BlobAsVarcharFct;

   public BytesConversionFcts() {
   }

   public static Collection<Function> all() {
      Collection<Function> functions = new ArrayList();
      CQL3Type.Native[] var1 = CQL3Type.Native.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         CQL3Type type = var1[var3];
         if(type != CQL3Type.Native.VARCHAR && type != CQL3Type.Native.BLOB) {
            functions.add(makeToBlobFunction(type.getType()));
            functions.add(makeFromBlobFunction(type.getType()));
         }
      }

      functions.add(VarcharAsBlobFct);
      functions.add(BlobAsVarcharFct);
      return functions;
   }

   public static Function makeToBlobFunction(AbstractType<?> fromType) {
      String name = fromType.asCQL3Type() + "asblob";
      return new BytesConversionFcts.BytesConversionFct(name, BytesType.instance, new AbstractType[]{fromType}) {
         public ByteBuffer execute(Arguments arguments) {
            return (ByteBuffer)arguments.get(0);
         }
      };
   }

   public static Function makeFromBlobFunction(final AbstractType<?> toType) {
      String name = "blobas" + toType.asCQL3Type();
      return new BytesConversionFcts.BytesConversionFct(name, toType, new AbstractType[]{BytesType.instance}) {
         public ByteBuffer execute(Arguments arguments) {
            ByteBuffer val = (ByteBuffer)arguments.get(0);

            try {
               if(val != null) {
                  toType.validate(val);
               }

               return val;
            } catch (MarshalException var4) {
               throw new InvalidRequestException(String.format("In call to function %s, value 0x%s is not a valid binary representation for type %s", new Object[]{this.name, ByteBufferUtil.bytesToHex(val), toType.asCQL3Type()}));
            }
         }
      };
   }

   static {
      VarcharAsBlobFct = new BytesConversionFcts.BytesConversionFct("varcharasblob", BytesType.instance, new AbstractType[]{UTF8Type.instance}) {
         public ByteBuffer execute(Arguments arguments) {
            return (ByteBuffer)arguments.get(0);
         }
      };
      BlobAsVarcharFct = new BytesConversionFcts.BytesConversionFct("blobasvarchar", UTF8Type.instance, new AbstractType[]{BytesType.instance}) {
         public ByteBuffer execute(Arguments arguments) {
            return (ByteBuffer)arguments.get(0);
         }
      };
   }

   private abstract static class BytesConversionFct extends NativeScalarFunction {
      public BytesConversionFct(String name, AbstractType<?> returnType, AbstractType... argsType) {
         super(name, returnType, argsType);
      }

      public Arguments newArguments(ProtocolVersion version) {
         return FunctionArguments.newNoopInstance(version, 1);
      }
   }
}
