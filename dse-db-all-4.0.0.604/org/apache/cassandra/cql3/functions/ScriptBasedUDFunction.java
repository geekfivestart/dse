package org.apache.cassandra.cql3.functions;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import jdk.nashorn.api.scripting.AbstractJSObject;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

final class ScriptBasedUDFunction extends UDFunction {
   private static final ProtectionDomain protectionDomain;
   private static final AccessControlContext accessControlContext;
   private static final String[] allowedPackagesArray = new String[]{"", "com", "edu", "java", "javax", "javafx", "org", "java.lang", "java.lang.invoke", "java.lang.reflect", "java.nio.charset", "java.util", "java.util.concurrent", "javax.script", "sun.reflect", "jdk.internal.org.objectweb.asm.commons", "jdk.nashorn.internal.runtime", "jdk.nashorn.internal.runtime.linker", "java.math", "java.nio", "java.text", "com.google.common.base", "com.google.common.collect", "com.google.common.reflect", "com.datastax.driver.core", "com.datastax.driver.core.utils"};
   private static final UDFExecutorService executor;
   private static final ClassFilter classFilter;
   private static final NashornScriptEngine scriptEngine;
   private final CompiledScript script;
   private final Object udfContextBinding;

   ScriptBasedUDFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      super(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, monotonicOn);
      if("JavaScript".equalsIgnoreCase(language) && scriptEngine != null) {
         try {
            this.script = AccessController.doPrivileged((PrivilegedExceptionAction<CompiledScript>)(() -> scriptEngine.compile(body)), accessControlContext);
         } catch (PrivilegedActionException var13) {
            Throwable e = var13.getCause();
            logger.info("Failed to compile function '{}' for language {}: ", new Object[]{name, language, e});
            throw new InvalidRequestException(String.format("Failed to compile function '%s' for language %s: %s", new Object[]{name, language, e}));
         }

         this.udfContextBinding = new ScriptBasedUDFunction.UDFContextWrapper();
      } else {
         throw new InvalidRequestException(String.format("Invalid language '%s' for function '%s'", new Object[]{language, name}));
      }
   }

   protected ExecutorService executor() {
      return executor;
   }

   public ByteBuffer executeUserDefined(Arguments arguments) {
      int size = this.argTypes.size();
      Object[] params = new Object[size];

      for(int i = 0; i < size; ++i) {
         params[i] = arguments.get(i);
      }

      Object result = this.executeScriptInternal(params);
      return this.decompose(arguments.getProtocolVersion(), result);
   }

   protected Object executeAggregateUserDefined(Object firstParam, Arguments arguments) {
      Object[] params = new Object[this.argTypes.size()];
      params[0] = firstParam;

      for(int i = 1; i < params.length; ++i) {
         params[i] = arguments.get(i - 1);
      }

      return this.executeScriptInternal(params);
   }

   private Object executeScriptInternal(Object[] params) {
      ScriptContext scriptContext = new SimpleScriptContext();
      scriptContext.setAttribute("javax.script.filename", this.name.toString(), 100);
      Bindings bindings = scriptContext.getBindings(100);

      for(int i = 0; i < params.length; ++i) {
         bindings.put(((ColumnIdentifier)this.argNames.get(i)).toString(), params[i]);
      }

      bindings.put("udfContext", this.udfContextBinding);

      Object result;
      try {
         result = this.script.eval(scriptContext);
      } catch (ScriptException var6) {
         throw new RuntimeException(var6);
      }

      return result == null?null:convert(result, this.resultType);
   }

   private static Object convert(Object obj, UDFDataType udfDataType) {
      Class<?> resultType = obj.getClass();
      Class<?> type = udfDataType.toJavaClass();
      if(type.isAssignableFrom(resultType)) {
         return obj;
      } else if(obj instanceof Number) {
         Number number = (Number)obj;
         return type == Integer.class?Integer.valueOf(number.intValue()):(type == Long.class?Long.valueOf(number.longValue()):(type == Short.class?Short.valueOf(number.shortValue()):(type == Byte.class?Byte.valueOf(number.byteValue()):(type == Float.class?Float.valueOf(number.floatValue()):(type == Double.class?Double.valueOf(number.doubleValue()):(type == BigInteger.class?(number instanceof BigDecimal?((BigDecimal)number).toBigInteger():(!(number instanceof Double) && !(number instanceof Float)?BigInteger.valueOf(number.longValue()):(new BigDecimal(number.toString())).toBigInteger())):new BigDecimal(number.toString())))))));
      } else {
         throw new InvalidTypeException("Invalid value for CQL type " + udfDataType.toDataType().getName());
      }
   }

   static {
      executor = new UDFExecutorService(new NamedThreadFactory("UserDefinedScriptFunctions", 1, udfClassLoader, new SecurityThreadGroup("UserDefinedScriptFunctions", Collections.unmodifiableSet(new HashSet(Arrays.asList(allowedPackagesArray))), UDFunction::initializeThread)), "userscripts");
      classFilter = (clsName) -> {
         return secureResource(clsName.replace('.', '/') + ".class");
      };
      ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
      ScriptEngine engine = scriptEngineManager.getEngineByName("nashorn");
      NashornScriptEngineFactory factory = engine != null?(NashornScriptEngineFactory)engine.getFactory():null;
      scriptEngine = factory != null?(NashornScriptEngine)factory.getScriptEngine(new String[0], udfClassLoader, classFilter):null;

      try {
         protectionDomain = new ProtectionDomain(new CodeSource(new URL("udf", "localhost", 0, "/script", new URLStreamHandler() {
            protected URLConnection openConnection(URL u) {
               return null;
            }
         }), (Certificate[])null), ThreadAwareSecurityManager.noPermissions);
      } catch (MalformedURLException var4) {
         throw new RuntimeException(var4);
      }

      accessControlContext = new AccessControlContext(new ProtectionDomain[]{protectionDomain});
   }

   private final class UDFContextWrapper extends AbstractJSObject {
      private final AbstractJSObject fRetUDT = new AbstractJSObject() {
         public Object call(Object thiz, Object... args) {
            return ScriptBasedUDFunction.this.udfContext.newReturnUDTValue();
         }
      };
      private final AbstractJSObject fArgUDT = new AbstractJSObject() {
         public Object call(Object thiz, Object... args) {
            return args[0] instanceof String?ScriptBasedUDFunction.this.udfContext.newArgUDTValue((String)args[0]):(args[0] instanceof Number?ScriptBasedUDFunction.this.udfContext.newArgUDTValue(((Number)args[0]).intValue()):super.call(thiz, args));
         }
      };
      private final AbstractJSObject fRetTup = new AbstractJSObject() {
         public Object call(Object thiz, Object... args) {
            return ScriptBasedUDFunction.this.udfContext.newReturnTupleValue();
         }
      };
      private final AbstractJSObject fArgTup = new AbstractJSObject() {
         public Object call(Object thiz, Object... args) {
            return args[0] instanceof String?ScriptBasedUDFunction.this.udfContext.newArgTupleValue((String)args[0]):(args[0] instanceof Number?ScriptBasedUDFunction.this.udfContext.newArgTupleValue(((Number)args[0]).intValue()):super.call(thiz, args));
         }
      };

      UDFContextWrapper() {
      }

      public Object getMember(String name) {
         byte var3 = -1;
         switch(name.hashCode()) {
         case -1560099905:
            if(name.equals("newArgTupleValue")) {
               var3 = 3;
            }
            break;
         case 93422524:
            if(name.equals("newReturnUDTValue")) {
               var3 = 0;
            }
            break;
         case 403494754:
            if(name.equals("newArgUDTValue")) {
               var3 = 1;
            }
            break;
         case 1108197785:
            if(name.equals("newReturnTupleValue")) {
               var3 = 2;
            }
         }

         switch(var3) {
         case 0:
            return this.fRetUDT;
         case 1:
            return this.fArgUDT;
         case 2:
            return this.fRetTup;
         case 3:
            return this.fArgTup;
         default:
            return super.getMember(name);
         }
      }
   }
}
