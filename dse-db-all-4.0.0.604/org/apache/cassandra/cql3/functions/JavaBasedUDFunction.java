package org.apache.cassandra.cql3.functions;

import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Principal;
import java.security.ProtectionDomain;
import java.security.SecureClassLoader;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.text.WordUtils;
import org.eclipse.jdt.core.compiler.IProblem;
import org.eclipse.jdt.internal.compiler.ClassFile;
import org.eclipse.jdt.internal.compiler.CompilationResult;
import org.eclipse.jdt.internal.compiler.Compiler;
import org.eclipse.jdt.internal.compiler.DefaultErrorHandlingPolicies;
import org.eclipse.jdt.internal.compiler.ICompilerRequestor;
import org.eclipse.jdt.internal.compiler.IErrorHandlingPolicy;
import org.eclipse.jdt.internal.compiler.IProblemFactory;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFileReader;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFormatException;
import org.eclipse.jdt.internal.compiler.env.AccessRestriction;
import org.eclipse.jdt.internal.compiler.env.ICompilationUnit;
import org.eclipse.jdt.internal.compiler.env.INameEnvironment;
import org.eclipse.jdt.internal.compiler.env.NameEnvironmentAnswer;
import org.eclipse.jdt.internal.compiler.impl.CompilerOptions;
import org.eclipse.jdt.internal.compiler.problem.DefaultProblemFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JavaBasedUDFunction extends UDFunction {
   private static final String BASE_PACKAGE = "org.apache.cassandra.cql3.udf.gen";
   static final Logger logger = LoggerFactory.getLogger(JavaBasedUDFunction.class);
   private static final AtomicInteger classSequence = new AtomicInteger();
   private static final UDFExecutorService executor;
   private static final JavaBasedUDFunction.EcjTargetClassLoader targetClassLoader;
   private static final UDFByteCodeVerifier udfByteCodeVerifier;
   private static final ProtectionDomain protectionDomain;
   private static final IErrorHandlingPolicy errorHandlingPolicy;
   private static final IProblemFactory problemFactory;
   private static final CompilerOptions compilerOptions;
   private static final String[] javaSourceTemplate;
   private final JavaUDF javaUDF;

   JavaBasedUDFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      super(name, argNames, argTypes, returnType, calledOnNullInput, "java", body, deterministic, monotonic, monotonicOn);
      String pkgName = "org.apache.cassandra.cql3.udf.gen." + generateClassName(name, 'p');
      String clsName = generateClassName(name, 'C');
      String executeInternalName = generateClassName(name, 'x');
      StringBuilder javaSourceBuilder = new StringBuilder();
      int lineOffset = 1;

      String javaSource;
      for(int i = 0; i < javaSourceTemplate.length; ++i) {
         javaSource = javaSourceTemplate[i];
         if((i & 1) == 1) {
            byte var18 = -1;
            switch(javaSource.hashCode()) {
            case -2035517098:
               if(javaSource.equals("arguments")) {
                  var18 = 3;
               }
               break;
            case -1877165340:
               if(javaSource.equals("package_name")) {
                  var18 = 0;
               }
               break;
            case -1486239511:
               if(javaSource.equals("return_type")) {
                  var18 = 6;
               }
               break;
            case -290505194:
               if(javaSource.equals("arguments_aggregate")) {
                  var18 = 4;
               }
               break;
            case -290474766:
               if(javaSource.equals("class_name")) {
                  var18 = 1;
               }
               break;
            case -40060704:
               if(javaSource.equals("argument_list")) {
                  var18 = 5;
               }
               break;
            case 3029410:
               if(javaSource.equals("body")) {
                  var18 = 2;
               }
               break;
            case 2052076419:
               if(javaSource.equals("execute_internal_name")) {
                  var18 = 7;
               }
            }

            switch(var18) {
            case 0:
               javaSource = pkgName;
               break;
            case 1:
               javaSource = clsName;
               break;
            case 2:
               lineOffset = countNewlines(javaSourceBuilder);
               javaSource = body;
               break;
            case 3:
               javaSource = generateArguments(this.argumentTypes, argNames, false);
               break;
            case 4:
               javaSource = generateArguments(this.argumentTypes, argNames, true);
               break;
            case 5:
               javaSource = generateArgumentList(this.argumentTypes, argNames);
               break;
            case 6:
               javaSource = this.resultType.getJavaTypeName();
               break;
            case 7:
               javaSource = executeInternalName;
            }
         }

         javaSourceBuilder.append(javaSource);
      }

      String targetClassName = pkgName + '.' + clsName;
      javaSource = javaSourceBuilder.toString();
      logger.trace("Compiling Java source UDF '{}' as class '{}' using source:\n{}", new Object[]{name, targetClassName, javaSource});

      try {
         JavaBasedUDFunction.EcjCompilationUnit compilationUnit = new JavaBasedUDFunction.EcjCompilationUnit(javaSource, targetClassName);
         Compiler compiler = new Compiler(compilationUnit, errorHandlingPolicy, compilerOptions, compilationUnit, problemFactory);
         compiler.compile(new ICompilationUnit[]{compilationUnit});
         Iterator i;
         if(compilationUnit.problemList != null && !compilationUnit.problemList.isEmpty()) {
            boolean fullSource = false;
            StringBuilder problems = new StringBuilder();
            i = compilationUnit.problemList.iterator();

            while(i.hasNext()) {
               IProblem problem = (IProblem)i.next();
               long ln = (long)(problem.getSourceLineNumber() - lineOffset);
               if(ln < 1L) {
                  if(problem.isError()) {
                     problems.append("GENERATED SOURCE ERROR: line ").append(problem.getSourceLineNumber()).append(" (in generated source): ").append(problem.getMessage()).append('\n');
                     fullSource = true;
                  }
               } else {
                  problems.append("Line ").append(Long.toString(ln)).append(": ").append(problem.getMessage()).append('\n');
               }
            }

            if(fullSource) {
               throw new InvalidRequestException("Java source compilation failed:\n" + problems + "\n generated source:\n" + javaSource);
            } else {
               throw new InvalidRequestException("Java source compilation failed:\n" + problems);
            }
         } else {
            Set<String> errors = udfByteCodeVerifier.verify(targetClassName, targetClassLoader.classData(targetClassName));
            String validDeclare = "not allowed method declared: " + executeInternalName + '(';
            i = errors.iterator();

            while(i.hasNext()) {
               String error = (String)i.next();
               if(error.startsWith(validDeclare)) {
                  i.remove();
               }
            }

            if(!errors.isEmpty()) {
               throw new InvalidRequestException("Java UDF validation failed: " + errors);
            } else {
               Thread thread = Thread.currentThread();
               ClassLoader orig = thread.getContextClassLoader();

               try {
                  thread.setContextClassLoader(UDFunction.udfClassLoader);
                  Class cls = Class.forName(targetClassName, false, targetClassLoader);
                  int nonSyntheticMethodCount = 0;
                  Method[] var25 = cls.getDeclaredMethods();
                  int var26 = var25.length;

                  for(int var27 = 0; var27 < var26; ++var27) {
                     Method m = var25[var27];
                     if(!m.isSynthetic()) {
                        ++nonSyntheticMethodCount;
                     }
                  }

                  if(nonSyntheticMethodCount != 3 || cls.getDeclaredConstructors().length != 1) {
                     throw new InvalidRequestException("Check your source to not define additional Java methods or constructors");
                  }

                  MethodType methodType = MethodType.methodType(Void.TYPE).appendParameterTypes(new Class[]{UDFDataType.class, UDFContext.class});
                  MethodHandle ctor = MethodHandles.lookup().findConstructor(cls, methodType);
                  this.javaUDF = (JavaUDF)ctor.invokeWithArguments(new Object[]{this.resultType, this.udfContext});
               } finally {
                  thread.setContextClassLoader(orig);
               }

            }
         }
      } catch (InvocationTargetException var35) {
         throw new InvalidRequestException(String.format("Could not compile function '%s' from Java source: %s", new Object[]{name, var35.getCause()}));
      } catch (VirtualMachineError | InvalidRequestException var36) {
         throw var36;
      } catch (Throwable var37) {
         logger.error(String.format("Could not compile function '%s' from Java source:%n%s", new Object[]{name, javaSource}), var37);
         throw new InvalidRequestException(String.format("Could not compile function '%s' from Java source: %s", new Object[]{name, var37}));
      }
   }

   protected ExecutorService executor() {
      return executor;
   }

   protected ByteBuffer executeUserDefined(Arguments arguments) {
      return this.javaUDF.executeImpl(arguments);
   }

   protected Object executeAggregateUserDefined(Object state, Arguments arguments) {
      return this.javaUDF.executeAggregateImpl(state, arguments);
   }

   private static int countNewlines(StringBuilder javaSource) {
      int ln = 0;

      for(int i = 0; i < javaSource.length(); ++i) {
         if(javaSource.charAt(i) == 10) {
            ++ln;
         }
      }

      return ln;
   }

   private static String generateClassName(FunctionName name, char prefix) {
      String qualifiedName = name.toString();
      StringBuilder sb = new StringBuilder(qualifiedName.length() + 10);
      sb.append(prefix);

      for(int i = 0; i < qualifiedName.length(); ++i) {
         char c = qualifiedName.charAt(i);
         if(Character.isJavaIdentifierPart(c)) {
            sb.append(c);
         } else {
            sb.append(Integer.toHexString((short)c & '\uffff'));
         }
      }

      sb.append('_').append(ThreadLocalRandom.current().nextInt() & 16777215).append('_').append(classSequence.incrementAndGet());
      return sb.toString();
   }

   private static String generateArgumentList(List<UDFDataType> argTypes, List<ColumnIdentifier> argNames) {
      StringBuilder code = new StringBuilder(32 * argTypes.size());

      for(int i = 0; i < argTypes.size(); ++i) {
         if(i > 0) {
            code.append(", ");
         }

         code.append(((UDFDataType)argTypes.get(i)).getJavaTypeName()).append(' ').append(argNames.get(i));
      }

      return code.toString();
   }

   private static String generateArguments(List<UDFDataType> argTypes, List<ColumnIdentifier> argNames, boolean forAggregate) {
      int size = argTypes.size();
      StringBuilder code = new StringBuilder(64 * size);

      for(int i = 0; i < size; ++i) {
         UDFDataType argType = (UDFDataType)argTypes.get(i);
         if(i > 0) {
            code.append(",\n");
         }

         if(logger.isTraceEnabled()) {
            code.append("            /* argument '").append(argNames.get(i)).append("' */\n");
         }

         code.append("            ");
         code.append('(').append(argType.getJavaTypeName()).append(") ");
         if(forAggregate && i == 0) {
            code.append("state");
         } else {
            code.append("arguments.");
            appendGetMethodName(code, argType).append('(').append(forAggregate?i - 1:i).append(')');
         }
      }

      return code.toString();
   }

   private static StringBuilder appendGetMethodName(StringBuilder code, UDFDataType type) {
      code.append("get");
      return !type.isPrimitive()?code:code.append("As").append(WordUtils.capitalize(type.getJavaTypeName()));
   }

   static {
      executor = new UDFExecutorService(new NamedThreadFactory("UserDefinedFunctions", 1, udfClassLoader, new SecurityThreadGroup("UserDefinedFunctions", (Set)null, UDFunction::initializeThread)), "userfunction");
      targetClassLoader = new JavaBasedUDFunction.EcjTargetClassLoader();
      udfByteCodeVerifier = new UDFByteCodeVerifier();
      errorHandlingPolicy = DefaultErrorHandlingPolicies.proceedWithAllProblems();
      problemFactory = new DefaultProblemFactory(Locale.ENGLISH);
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "forName");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "getClassLoader");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "getResource");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "getResourceAsStream");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "clearAssertionStatus");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getResource");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getResourceAsStream");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getResources");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemClassLoader");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemResource");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemResourceAsStream");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemResources");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "loadClass");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "setClassAssertionStatus");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "setDefaultAssertionStatus");
      udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "setPackageAssertionStatus");
      udfByteCodeVerifier.addDisallowedMethodCall("java/nio/ByteBuffer", "allocateDirect");
      String[] var0 = new String[]{"java/net/InetAddress", "java/net/Inet4Address", "java/net/Inet6Address"};
      int var1 = var0.length;

      for(int var2 = 0; var2 < var1; ++var2) {
         String ia = var0[var2];
         udfByteCodeVerifier.addDisallowedMethodCall(ia, "getByAddress");
         udfByteCodeVerifier.addDisallowedMethodCall(ia, "getAllByName");
         udfByteCodeVerifier.addDisallowedMethodCall(ia, "getByName");
         udfByteCodeVerifier.addDisallowedMethodCall(ia, "getLocalHost");
         udfByteCodeVerifier.addDisallowedMethodCall(ia, "getHostName");
         udfByteCodeVerifier.addDisallowedMethodCall(ia, "getCanonicalHostName");
         udfByteCodeVerifier.addDisallowedMethodCall(ia, "isReachable");
      }

      udfByteCodeVerifier.addDisallowedClass("java/net/NetworkInterface");
      udfByteCodeVerifier.addDisallowedClass("java/net/SocketException");
      Map<String, String> settings = new HashMap();
      settings.put("org.eclipse.jdt.core.compiler.debug.lineNumber", "generate");
      settings.put("org.eclipse.jdt.core.compiler.debug.sourceFile", "disabled");
      settings.put("org.eclipse.jdt.core.compiler.problem.deprecation", "ignore");
      settings.put("org.eclipse.jdt.core.compiler.source", "1.8");
      settings.put("org.eclipse.jdt.core.compiler.codegen.targetPlatform", "1.8");
      compilerOptions = new CompilerOptions(settings);
      compilerOptions.parseLiteralExpressionsAsConstants = true;

      try {
         InputStream input = JavaBasedUDFunction.class.getResource("JavaSourceUDF.txt").openConnection().getInputStream();
         Throwable var23 = null;

         try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            FBUtilities.copy(input, output, 9223372036854775807L);
            String template = output.toString();
            StringTokenizer st = new StringTokenizer(template, "#");
            javaSourceTemplate = new String[st.countTokens()];

            for(int i = 0; st.hasMoreElements(); ++i) {
               javaSourceTemplate[i] = st.nextToken();
            }
         } catch (Throwable var17) {
            var23 = var17;
            throw var17;
         } finally {
            if(input != null) {
               if(var23 != null) {
                  try {
                     input.close();
                  } catch (Throwable var15) {
                     var23.addSuppressed(var15);
                  }
               } else {
                  input.close();
               }
            }

         }
      } catch (IOException var19) {
         throw new RuntimeException(var19);
      }

      CodeSource codeSource;
      try {
         codeSource = new CodeSource(new URL("udf", "localhost", 0, "/java", new URLStreamHandler() {
            protected URLConnection openConnection(URL u) {
               return null;
            }
         }), (Certificate[])null);
      } catch (MalformedURLException var16) {
         throw new RuntimeException(var16);
      }

      protectionDomain = new ProtectionDomain(codeSource, ThreadAwareSecurityManager.noPermissions, targetClassLoader, (Principal[])null);
   }

   static final class EcjTargetClassLoader extends SecureClassLoader {
      private final Map<String, byte[]> classes = new ConcurrentHashMap();

      EcjTargetClassLoader() {
         super(UDFunction.udfClassLoader);
      }

      void addClass(String className, byte[] classData) {
         this.classes.put(className, classData);
      }

      byte[] classData(String className) {
         return (byte[])this.classes.get(className);
      }

      protected Class<?> findClass(String name) throws ClassNotFoundException {
         byte[] classData = (byte[])this.classes.remove(name);
         return classData != null?this.defineClass(name, classData, 0, classData.length, JavaBasedUDFunction.protectionDomain):this.getParent().loadClass(name);
      }

      protected PermissionCollection getPermissions(CodeSource codesource) {
         return ThreadAwareSecurityManager.noPermissions;
      }
   }

   static final class EcjCompilationUnit implements ICompilationUnit, ICompilerRequestor, INameEnvironment {
      List<IProblem> problemList;
      private final String className;
      private final char[] sourceCode;

      EcjCompilationUnit(String sourceCode, String className) {
         this.className = className;
         this.sourceCode = sourceCode.toCharArray();
      }

      public char[] getFileName() {
         return this.sourceCode;
      }

      public char[] getContents() {
         return this.sourceCode;
      }

      public char[] getMainTypeName() {
         int dot = this.className.lastIndexOf(46);
         return (dot > 0?this.className.substring(dot + 1):this.className).toCharArray();
      }

      public char[][] getPackageName() {
         StringTokenizer izer = new StringTokenizer(this.className, ".");
         char[][] result = new char[izer.countTokens() - 1][];

         for(int i = 0; i < result.length; ++i) {
            result[i] = izer.nextToken().toCharArray();
         }

         return result;
      }

      public boolean ignoreOptionalProblems() {
         return false;
      }

      public void acceptResult(CompilationResult result) {
         if(result.hasErrors()) {
            IProblem[] problems = result.getProblems();
            if(this.problemList == null) {
               this.problemList = new ArrayList(problems.length);
            }

            Collections.addAll(this.problemList, problems);
         } else {
            ClassFile[] classFiles = result.getClassFiles();
            ClassFile[] var3 = classFiles;
            int var4 = classFiles.length;

            for(int var5 = 0; var5 < var4; ++var5) {
               ClassFile classFile = var3[var5];
               JavaBasedUDFunction.targetClassLoader.addClass(this.className, classFile.getBytes());
            }
         }

      }

      public NameEnvironmentAnswer findType(char[][] compoundTypeName) {
         StringBuilder result = new StringBuilder();

         for(int i = 0; i < compoundTypeName.length; ++i) {
            if(i > 0) {
               result.append('.');
            }

            result.append(compoundTypeName[i]);
         }

         return this.findType(result.toString());
      }

      public NameEnvironmentAnswer findType(char[] typeName, char[][] packageName) {
         StringBuilder result = new StringBuilder();

         int i;
         for(i = 0; i < packageName.length; ++i) {
            if(i > 0) {
               result.append('.');
            }

            result.append(packageName[i]);
         }

         if(i > 0) {
            result.append('.');
         }

         result.append(typeName);
         return this.findType(result.toString());
      }

      private NameEnvironmentAnswer findType(String className) {
         if(className.equals(this.className)) {
            return new NameEnvironmentAnswer(this, (AccessRestriction)null);
         } else {
            String resourceName = className.replace('.', '/') + ".class";

            try {
               InputStream is = UDFunction.udfClassLoader.getResourceAsStream(resourceName);
               Throwable var4 = null;

               NameEnvironmentAnswer var8;
               try {
                  if(is == null) {
                     return null;
                  }

                  byte[] classBytes = ByteStreams.toByteArray(is);
                  char[] fileName = className.toCharArray();
                  ClassFileReader classFileReader = new ClassFileReader(classBytes, fileName, true);
                  var8 = new NameEnvironmentAnswer(classFileReader, (AccessRestriction)null);
               } catch (Throwable var19) {
                  var4 = var19;
                  throw var19;
               } finally {
                  if(is != null) {
                     if(var4 != null) {
                        try {
                           is.close();
                        } catch (Throwable var18) {
                           var4.addSuppressed(var18);
                        }
                     } else {
                        is.close();
                     }
                  }

               }

               return var8;
            } catch (ClassFormatException | IOException var21) {
               throw new RuntimeException(var21);
            }
         }
      }

      private boolean isPackage(String result) {
         if(result.equals(this.className)) {
            return false;
         } else {
            String resourceName = result.replace('.', '/') + ".class";

            try {
               InputStream is = UDFunction.udfClassLoader.getResourceAsStream(resourceName);
               Throwable var4 = null;

               boolean var5;
               try {
                  var5 = is == null;
               } catch (Throwable var15) {
                  var4 = var15;
                  throw var15;
               } finally {
                  if(is != null) {
                     if(var4 != null) {
                        try {
                           is.close();
                        } catch (Throwable var14) {
                           var4.addSuppressed(var14);
                        }
                     } else {
                        is.close();
                     }
                  }

               }

               return var5;
            } catch (IOException var17) {
               return false;
            }
         }
      }

      public boolean isPackage(char[][] parentPackageName, char[] packageName) {
         StringBuilder result = new StringBuilder();
         int i = 0;
         if(parentPackageName != null) {
            while(i < parentPackageName.length) {
               if(i > 0) {
                  result.append('.');
               }

               result.append(parentPackageName[i]);
               ++i;
            }
         }

         if(Character.isUpperCase(packageName[0]) && !this.isPackage(result.toString())) {
            return false;
         } else {
            if(i > 0) {
               result.append('.');
            }

            result.append(packageName);
            return this.isPackage(result.toString());
         }
      }

      public void cleanup() {
      }
   }
}
