package org.apache.cassandra.cql3.functions;

import com.datastax.driver.core.TypeCodec;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UDFunction extends AbstractFunction implements ScalarFunction {
   protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);
   static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
   protected final List<ColumnIdentifier> argNames;
   protected final String language;
   protected final String body;
   protected final boolean deterministic;
   protected final boolean monotonic;
   protected final List<ColumnIdentifier> monotonicOn;
   protected final List<UDFDataType> argumentTypes;
   protected final UDFDataType resultType;
   protected final boolean calledOnNullInput;
   protected final UDFContext udfContext;
   private static final String[] whitelistedPatterns = new String[]{"com/datastax/driver/core/", "com/google/common/reflect/TypeToken", "java/io/IOException.class", "java/io/Serializable.class", "java/lang/", "java/math/", "java/net/InetAddress.class", "java/net/Inet4Address.class", "java/net/Inet6Address.class", "java/net/UnknownHostException.class", "java/net/NetworkInterface.class", "java/net/SocketException.class", "java/nio/Buffer.class", "java/nio/ByteBuffer.class", "java/text/", "java/time/", "java/util/", "org/apache/cassandra/cql3/functions/Arguments.class", "org/apache/cassandra/cql3/functions/UDFDataType.class", "org/apache/cassandra/cql3/functions/JavaUDF.class", "org/apache/cassandra/cql3/functions/UDFContext.class", "org/apache/cassandra/exceptions/", "org/apache/cassandra/transport/ProtocolVersion.class"};
   private static final String[] blacklistedPatterns = new String[]{"com/datastax/driver/core/Cluster.class", "com/datastax/driver/core/Metrics.class", "com/datastax/driver/core/NettyOptions.class", "com/datastax/driver/core/Session.class", "com/datastax/driver/core/Statement.class", "com/datastax/driver/core/TimestampGenerator.class", "java/lang/Compiler.class", "java/lang/InheritableThreadLocal.class", "java/lang/Package.class", "java/lang/Process.class", "java/lang/ProcessBuilder.class", "java/lang/ProcessEnvironment.class", "java/lang/ProcessImpl.class", "java/lang/Runnable.class", "java/lang/Runtime.class", "java/lang/Shutdown.class", "java/lang/Thread.class", "java/lang/ThreadGroup.class", "java/lang/ThreadLocal.class", "java/lang/instrument/", "java/lang/invoke/", "java/lang/management/", "java/lang/ref/", "java/lang/reflect/", "java/util/ServiceLoader.class", "java/util/Timer.class", "java/util/concurrent/", "java/util/function/", "java/util/jar/", "java/util/logging/", "java/util/prefs/", "java/util/spi/", "java/util/stream/", "java/util/zip/"};
   static final ClassLoader udfClassLoader = new UDFunction.UDFClassLoader(null);

   static boolean secureResource(String resource) {
      while(resource.startsWith("/")) {
         resource = resource.substring(1);
      }

      String[] var1 = whitelistedPatterns;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         String white = var1[var3];
         if(resource.startsWith(white)) {
            String[] var5 = blacklistedPatterns;
            int var6 = var5.length;

            for(int var7 = 0; var7 < var6; ++var7) {
               String black = var5[var7];
               if(resource.startsWith(black)) {
                  logger.trace("access denied: resource {}", resource);
                  return false;
               }
            }

            return true;
         }
      }

      logger.trace("access denied: resource {}", resource);
      return false;
   }

   protected UDFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      super(name, argTypes, returnType);

      assert (new HashSet(argNames)).size() == argNames.size() : "duplicate argument names";

      this.argNames = argNames;
      this.language = language;
      this.body = body;
      this.deterministic = deterministic;
      this.monotonic = monotonic;
      this.monotonicOn = monotonicOn;
      this.argumentTypes = UDFDataType.wrap(argTypes, !calledOnNullInput);
      this.resultType = UDFDataType.wrap(returnType, !calledOnNullInput);
      this.calledOnNullInput = calledOnNullInput;
      KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(name.keyspace);
      this.udfContext = new UDFContextImpl(argNames, this.argumentTypes, this.resultType, keyspaceMetadata);
   }

   public Arguments newArguments(ProtocolVersion version) {
      return FunctionArguments.newInstanceForUdf(version, this.argumentTypes);
   }

   public static UDFunction create(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      assertUdfsEnabled(language);
      byte var11 = -1;
      switch(language.hashCode()) {
      case 3254818:
         if(language.equals("java")) {
            var11 = 0;
         }
      default:
         switch(var11) {
         case 0:
            return new JavaBasedUDFunction(name, argNames, argTypes, returnType, calledOnNullInput, body, deterministic, monotonic, monotonicOn);
         default:
            return new ScriptBasedUDFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, monotonicOn);
         }
      }
   }

   public static UDFunction createBrokenFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn, final InvalidRequestException reason) {
      return new UDFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, monotonicOn) {
         protected ExecutorService executor() {
            return Executors.newSingleThreadExecutor();
         }

         protected Object executeAggregateUserDefined(Object firstParam, Arguments arguments) {
            throw this.broken();
         }

         public ByteBuffer executeUserDefined(Arguments arguments) {
            throw this.broken();
         }

         private InvalidRequestException broken() {
            return new InvalidRequestException(String.format("Function '%s' exists but hasn't been loaded successfully for the following reason: %s. Please see the server log for details", new Object[]{this, reason.getMessage()}));
         }
      };
   }

   public boolean isDeterministic() {
      return this.deterministic;
   }

   public boolean isMonotonic() {
      return this.monotonic;
   }

   public boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters) {
      assert partialParameters.size() == this.argNames.size();

      if(!this.monotonic) {
         for(int i = 0; i < partialParameters.size(); ++i) {
            ByteBuffer partialParameter = (ByteBuffer)partialParameters.get(i);
            if(partialParameter == Function.UNRESOLVED) {
               ColumnIdentifier unresolvedArgumentName = (ColumnIdentifier)this.argNames.get(i);
               if(!this.monotonicOn.contains(unresolvedArgumentName)) {
                  return false;
               }
            }
         }
      }

      return true;
   }

   public final ByteBuffer execute(Arguments arguments) {
      assertUdfsEnabled(this.language);
      if(!this.isCallableWrtNullable(arguments)) {
         return null;
      } else {
         long tStart = System.nanoTime();

         try {
            ByteBuffer result = DatabaseDescriptor.enableUserDefinedFunctionsThreads()?this.executeAsync(arguments):this.executeUserDefined(arguments);
            Tracing.trace("Executed UDF {} in {}μs", this.name(), Long.valueOf((System.nanoTime() - tStart) / 1000L));
            return result;
         } catch (InvalidRequestException var5) {
            throw var5;
         } catch (Throwable var6) {
            logger.trace("Invocation of user-defined function '{}' failed", this, var6);
            if(var6 instanceof VirtualMachineError) {
               throw (VirtualMachineError)var6;
            } else {
               throw FunctionExecutionException.create(this, var6);
            }
         }
      }
   }

   public final Object executeForAggregate(Object state, Arguments arguments) {
      assertUdfsEnabled(this.language);
      if((this.calledOnNullInput || state != null) && this.isCallableWrtNullable(arguments)) {
         long tStart = System.nanoTime();

         try {
            Object result = DatabaseDescriptor.enableUserDefinedFunctionsThreads()?this.executeAggregateAsync(state, arguments):this.executeAggregateUserDefined(state, arguments);
            Tracing.trace("Executed UDF {} in {}μs", this.name(), Long.valueOf((System.nanoTime() - tStart) / 1000L));
            return result;
         } catch (InvalidRequestException var6) {
            throw var6;
         } catch (Throwable var7) {
            logger.debug("Invocation of user-defined function '{}' failed", this, var7);
            if(var7 instanceof VirtualMachineError) {
               throw (VirtualMachineError)var7;
            } else {
               throw FunctionExecutionException.create(this, var7);
            }
         }
      } else {
         return null;
      }
   }

   public static void assertUdfsEnabled(String language) {
      if(!DatabaseDescriptor.enableUserDefinedFunctions()) {
         throw new InvalidRequestException("User-defined functions are disabled in cassandra.yaml - set enable_user_defined_functions=true to enable");
      } else if(!"java".equalsIgnoreCase(language) && !DatabaseDescriptor.enableScriptedUserDefinedFunctions()) {
         throw new InvalidRequestException("Scripted user-defined functions are disabled in cassandra.yaml - set enable_scripted_user_defined_functions=true to enable if you are aware of the security risks");
      }
   }

   static void initializeThread() {
      TypeCodec.inet().format(InetAddress.getLoopbackAddress());
      TypeCodec.ascii().format("");
   }

   private ByteBuffer executeAsync(Arguments arguments) {
      UDFunction.ThreadIdAndCpuTime threadIdAndCpuTime = new UDFunction.ThreadIdAndCpuTime();
      return (ByteBuffer)this.async(threadIdAndCpuTime, () -> {
         threadIdAndCpuTime.setup();
         return this.executeUserDefined(arguments);
      });
   }

   private Object executeAggregateAsync(Object state, Arguments arguments) {
      UDFunction.ThreadIdAndCpuTime threadIdAndCpuTime = new UDFunction.ThreadIdAndCpuTime();
      return this.async(threadIdAndCpuTime, () -> {
         threadIdAndCpuTime.setup();
         return this.executeAggregateUserDefined(state, arguments);
      });
   }

   private <T> T async(UDFunction.ThreadIdAndCpuTime threadIdAndCpuTime, Callable<T> callable) {
      Future future = this.executor().submit(callable);

      try {
         if(DatabaseDescriptor.getUserDefinedFunctionWarnTimeout() > 0L) {
            try {
               return future.get(DatabaseDescriptor.getUserDefinedFunctionWarnTimeout(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException var11) {
               String warn = String.format("User defined function %s ran longer than %dms", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionWarnTimeout())});
               logger.warn(warn);
               ClientWarn.instance.warn(warn);
            }
         }

         return future.get(DatabaseDescriptor.getUserDefinedFunctionFailTimeout() - DatabaseDescriptor.getUserDefinedFunctionWarnTimeout(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException var12) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(var12);
      } catch (ExecutionException var13) {
         Throwable c = var13.getCause();
         if(c instanceof RuntimeException) {
            throw (RuntimeException)c;
         } else {
            throw new RuntimeException(c);
         }
      } catch (TimeoutException var14) {
         try {
            threadIdAndCpuTime.get(1L, TimeUnit.SECONDS);
            long cpuTimeMillis = threadMXBean.getThreadCpuTime(threadIdAndCpuTime.threadId) - threadIdAndCpuTime.cpuTime;
            cpuTimeMillis /= 1000000L;
            return future.get(Math.max(DatabaseDescriptor.getUserDefinedFunctionFailTimeout() - cpuTimeMillis, 0L), TimeUnit.MILLISECONDS);
         } catch (InterruptedException var8) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(var14);
         } catch (ExecutionException var9) {
            Throwable c = var14.getCause();
            if(c instanceof RuntimeException) {
               throw (RuntimeException)c;
            } else {
               throw new RuntimeException(c);
            }
         } catch (TimeoutException var10) {
            TimeoutException cause = new TimeoutException(String.format("User defined function %s ran longer than %dms%s", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionFailTimeout()), DatabaseDescriptor.getUserFunctionTimeoutPolicy() == Config.UserFunctionTimeoutPolicy.ignore?"":" - will stop Cassandra VM"}));
            FunctionExecutionException fe = FunctionExecutionException.create(this, cause);
            JVMStabilityInspector.userFunctionTimeout(cause);
            throw fe;
         }
      }
   }

   protected abstract ExecutorService executor();

   public boolean isCallableWrtNullable(Arguments arguments) {
      return this.calledOnNullInput || !arguments.containsNulls();
   }

   protected abstract ByteBuffer executeUserDefined(Arguments var1);

   protected abstract Object executeAggregateUserDefined(Object var1, Arguments var2);

   public boolean isAggregate() {
      return false;
   }

   public boolean isNative() {
      return false;
   }

   public boolean isCalledOnNullInput() {
      return this.calledOnNullInput;
   }

   public List<ColumnIdentifier> argNames() {
      return this.argNames;
   }

   public String body() {
      return this.body;
   }

   public String language() {
      return this.language;
   }

   public List<ColumnIdentifier> monotonicOn() {
      return this.monotonicOn;
   }

   protected ByteBuffer decompose(ProtocolVersion protocolVersion, Object value) {
      return this.resultType.decompose(protocolVersion, value);
   }

   public boolean equals(Object o) {
      if(!(o instanceof UDFunction)) {
         return false;
      } else {
         UDFunction that = (UDFunction)o;
         return Objects.equals(this.name, that.name) && Objects.equals(this.argNames, that.argNames) && Functions.typesMatch(this.argTypes, that.argTypes) && Functions.typesMatch(this.returnType, that.returnType) && Objects.equals(this.language, that.language) && Objects.equals(this.body, that.body);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.name, Integer.valueOf(Functions.typeHashCode(this.argTypes)), this.returnType, this.language, this.body});
   }

   private static class UDFClassLoader extends ClassLoader {
      static final ClassLoader insecureClassLoader = Thread.currentThread().getContextClassLoader();

      private UDFClassLoader() {
      }

      public URL getResource(String name) {
         return !UDFunction.secureResource(name)?null:insecureClassLoader.getResource(name);
      }

      protected URL findResource(String name) {
         return this.getResource(name);
      }

      public Enumeration<URL> getResources(String name) {
         return Collections.emptyEnumeration();
      }

      protected Class<?> findClass(String name) throws ClassNotFoundException {
         if(!UDFunction.secureResource(name.replace('.', '/') + ".class")) {
            throw new ClassNotFoundException(name);
         } else {
            return insecureClassLoader.loadClass(name);
         }
      }

      public Class<?> loadClass(String name) throws ClassNotFoundException {
         if(!UDFunction.secureResource(name.replace('.', '/') + ".class")) {
            throw new ClassNotFoundException(name);
         } else {
            return super.loadClass(name);
         }
      }
   }

   private static final class ThreadIdAndCpuTime extends CompletableFuture<Object> {
      long threadId;
      long cpuTime;

      ThreadIdAndCpuTime() {
         UDFunction.threadMXBean.getCurrentThreadCpuTime();
      }

      void setup() {
         this.threadId = Thread.currentThread().getId();
         this.cpuTime = UDFunction.threadMXBean.getCurrentThreadCpuTime();
         this.complete((Object)null);
      }
   }
}
