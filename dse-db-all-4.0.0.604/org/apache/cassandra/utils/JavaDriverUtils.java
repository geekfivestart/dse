package org.apache.cassandra.utils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;

public final class JavaDriverUtils {
   private static final MethodHandle methodParseOne;

   public static TypeCodec<Object> codecFor(AbstractType<?> abstractType) {
      return codecFor(driverType(abstractType));
   }

   public static TypeCodec<Object> codecFor(DataType dataType) {
      return CodecRegistry.DEFAULT_INSTANCE.codecFor(dataType);
   }

   public static DataType driverType(AbstractType<?> abstractType) {
      return driverType(abstractType.toString());
   }

   public static DataType driverType(String abstractType) {
      try {
         return methodParseOne.invoke(abstractType, JavaDriverUtils.LatestDriverSupportedVersion.protocolVersion, CodecRegistry.DEFAULT_INSTANCE);
      } catch (Error | RuntimeException var2) {
         throw var2;
      } catch (Throwable var3) {
         throw new RuntimeException("cannot parse driver type " + abstractType, var3);
      }
   }

   private JavaDriverUtils() {
   }

   static {
      try {
         Class<?> cls = Class.forName("com.datastax.driver.core.DataTypeClassNameParser");
         Method m = cls.getDeclaredMethod("parseOne", new Class[]{String.class, ProtocolVersion.class, CodecRegistry.class});
         m.setAccessible(true);
         methodParseOne = MethodHandles.lookup().unreflect(m);
      } catch (Exception var2) {
         throw new RuntimeException(var2);
      }
   }

   public static final class LatestDriverSupportedVersion {
      public static final ProtocolVersion protocolVersion = newestSupportedProtocolVersion();

      private static ProtocolVersion newestSupportedProtocolVersion() {
         List<org.apache.cassandra.transport.ProtocolVersion> available = new ArrayList(org.apache.cassandra.transport.ProtocolVersion.SUPPORTED);
         int i = available.size() - 1;

         while(i >= 0) {
            try {
               org.apache.cassandra.transport.ProtocolVersion avail = (org.apache.cassandra.transport.ProtocolVersion)available.get(i);
               return ProtocolVersion.fromInt(avail.asInt());
            } catch (IllegalArgumentException var3) {
               --i;
            }
         }

         throw new AssertionError();
      }

      private LatestDriverSupportedVersion() {
      }
   }
}
