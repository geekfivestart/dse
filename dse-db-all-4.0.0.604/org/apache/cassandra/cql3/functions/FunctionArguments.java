package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

final class FunctionArguments implements Arguments {
   private static final FunctionArguments EMPTY;
   private final ArgumentDeserializer[] deserializers;
   private final ProtocolVersion version;
   private final Object[] arguments;

   public static FunctionArguments newInstanceForUdf(ProtocolVersion version, List<UDFDataType> argTypes) {
      int size = argTypes.size();
      if(size == 0) {
         return emptyInstance(version);
      } else {
         ArgumentDeserializer[] deserializers = new ArgumentDeserializer[size];

         for(int i = 0; i < size; ++i) {
            deserializers[i] = ((UDFDataType)argTypes.get(i)).getArgumentDeserializer();
         }

         return new FunctionArguments(version, deserializers);
      }
   }

   public ProtocolVersion getProtocolVersion() {
      return this.version;
   }

   public static FunctionArguments newNoopInstance(ProtocolVersion version, int numberOfArguments) {
      ArgumentDeserializer[] deserializers = new ArgumentDeserializer[numberOfArguments];
      Arrays.fill(deserializers, ArgumentDeserializer.NOOP_DESERIALIZER);
      return new FunctionArguments(version, deserializers);
   }

   public static FunctionArguments emptyInstance(ProtocolVersion version) {
      return version == ProtocolVersion.CURRENT?EMPTY:new FunctionArguments(version, new ArgumentDeserializer[0]);
   }

   public static FunctionArguments newInstanceForNativeFunction(ProtocolVersion version, List<AbstractType<?>> argTypes) {
      int size = argTypes.size();
      if(size == 0) {
         return emptyInstance(version);
      } else {
         ArgumentDeserializer[] deserializers = new ArgumentDeserializer[size];

         for(int i = 0; i < size; ++i) {
            deserializers[i] = ((AbstractType)argTypes.get(i)).getArgumentDeserializer();
         }

         return new FunctionArguments(version, deserializers);
      }
   }

   public FunctionArguments(ProtocolVersion version, ArgumentDeserializer... deserializers) {
      this.version = version;
      this.deserializers = deserializers;
      this.arguments = new Object[deserializers.length];
   }

   public void set(int i, ByteBuffer buffer) {
      this.arguments[i] = this.deserializers[i].deserialize(this.version, buffer);
   }

   public boolean containsNulls() {
      for(int i = 0; i < this.arguments.length; ++i) {
         if(this.arguments[i] == null) {
            return true;
         }
      }

      return false;
   }

   public <T> T get(int i) {
      return this.arguments[i];
   }

   public int size() {
      return this.arguments.length;
   }

   static {
      EMPTY = new FunctionArguments(ProtocolVersion.CURRENT, new ArgumentDeserializer[0]);
   }
}
