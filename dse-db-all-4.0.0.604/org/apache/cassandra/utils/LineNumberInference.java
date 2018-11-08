package org.apache.cassandra.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LineNumberInference {
   public static final LineNumberInference.Descriptor UNKNOWN_SOURCE = new LineNumberInference.Descriptor("", "unknown", -1);
   private static final Logger logger = LoggerFactory.getLogger(LineNumberInference.class);
   private static final String INIT = "<init>";
   private static final Pattern LAMBDA_PATTERN = Pattern.compile("(.*)\\$\\$(Lambda)\\$(\\d+)/(\\d+)");
   private static final String DUMP_CLASSES = "jdk.internal.lambda.dumpProxyClasses";
   private static final String tmpDir;
   private final Map<Class<?>, LineNumberInference.Descriptor> descriptors = new ConcurrentHashMap();
   private boolean preloaded;

   public static void init() {
   }

   public LineNumberInference() {
   }

   public void preloadLambdas() {
      if(!this.preloaded) {
         this.preloaded = true;
         if(tmpDir != null) {
            try {
               PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**.class");
               Path tmpFilePath = (new File(tmpDir)).toPath();
               String lookupRegex = tmpFilePath + "\\/(.*)\\.class";
               Collection<File> files = (Collection)Files.find(tmpFilePath, 999, (path, attributes) -> {
                  return matcher.matches(path);
               }, new FileVisitOption[0]).map(Path::toFile).collect(Collectors.toList());
               Iterator var5 = files.iterator();

               while(var5.hasNext()) {
                  File file = (File)var5.next();
                  String className = slashToDot(file.getAbsolutePath().replaceFirst(lookupRegex, "$1"));

                  try {
                     InputStream in = new FileInputStream(file);
                     Throwable var9 = null;

                     try {
                        LineNumberInference.LambdaClassVisitor clsVisitor = new LineNumberInference.LambdaClassVisitor();
                        (new ClassReader(in)).accept(clsVisitor, 4);

                        try {
                           Class<?> klass = Class.forName(className, false, this.getClass().getClassLoader());
                           this.processLambdaImplementationMethod(klass, clsVisitor);
                        } catch (ClassNotFoundException var22) {
                           ;
                        }
                     } catch (Throwable var23) {
                        var9 = var23;
                        throw var23;
                     } finally {
                        if(in != null) {
                           if(var9 != null) {
                              try {
                                 in.close();
                              } catch (Throwable var21) {
                                 var9.addSuppressed(var21);
                              }
                           } else {
                              in.close();
                           }
                        }

                     }
                  } catch (IOException var25) {
                     logger.warn("Failed to preload lambdas: rich stack traces for flows will be disabled. ", var25);
                  }
               }
            } catch (Throwable var26) {
               logger.warn("Couldn't load the list of lambdas for rich stack traces.", var26);
            }

         }
      }
   }

   public LineNumberInference.Descriptor getLine(Class klass) {
      LineNumberInference.Descriptor descriptor = (LineNumberInference.Descriptor)this.descriptors.get(klass);
      if(descriptor == null) {
         if(this.maybeProcessClassInternal(klass)) {
            descriptor = (LineNumberInference.Descriptor)this.descriptors.get(klass);
         }

         if(descriptor == null) {
            logger.trace("Could not find line information for " + klass);
            return UNKNOWN_SOURCE;
         }
      }

      return descriptor;
   }

   boolean maybeProcessClass(Class klass) {
      try {
         return this.descriptors.containsKey(klass)?false:this.maybeProcessClassInternal(klass);
      } catch (Throwable var3) {
         this.descriptors.put(klass, UNKNOWN_SOURCE);
         logger.debug("Could not process class information for lambda {}", klass.getName(), var3);
         logger.warn("Could not process class information for lambda {}", klass.getName());
         return false;
      }
   }

   private boolean maybeProcessClassInternal(Class klass) {
      if(tmpDir == null) {
         return false;
      } else {
         String className = klass.getName();

         try {
            if(LAMBDA_PATTERN.matcher(className).matches()) {
               String parentClass = className.split("/")[0];
               LineNumberInference.LambdaClassVisitor clsVisitor = new LineNumberInference.LambdaClassVisitor();
               InputStream in = new FileInputStream(tmpDir + '/' + dotToSlash(parentClass) + ".class");
               Throwable var6 = null;

               try {
                  (new ClassReader(in)).accept(clsVisitor, 4);
               } catch (Throwable var31) {
                  var6 = var31;
                  throw var31;
               } finally {
                  if(in != null) {
                     if(var6 != null) {
                        try {
                           in.close();
                        } catch (Throwable var30) {
                           var6.addSuppressed(var30);
                        }
                     } else {
                        in.close();
                     }
                  }

               }

               this.processLambdaImplementationMethod(klass, clsVisitor);
            } else {
               InputStream in = klass.getResourceAsStream('/' + dotToSlash(className) + ".class");
               Throwable var37 = null;

               try {
                  LineNumberInference.SimpleClassVisitor visitor = new LineNumberInference.SimpleClassVisitor();
                  (new ClassReader(in)).accept(visitor, 4);
                  this.descriptors.put(klass, visitor.descriptor());
               } catch (Throwable var32) {
                  var37 = var32;
                  throw var32;
               } finally {
                  if(in != null) {
                     if(var37 != null) {
                        try {
                           in.close();
                        } catch (Throwable var29) {
                           var37.addSuppressed(var29);
                        }
                     } else {
                        in.close();
                     }
                  }

               }
            }

            return true;
         } catch (IOException var35) {
            throw new RuntimeException("Failed to process class " + className, var35);
         }
      }
   }

   private void processLambdaImplementationMethod(Class klass, LineNumberInference.LambdaClassVisitor ref) throws IOException {
      String resourceName = '/' + dotToSlash(ref.lambdaClass) + ".class";
      InputStream in = klass.getResourceAsStream(resourceName);
      Throwable var5 = null;

      try {
         LineNumberInference.LambdaImplementationVisitor implVisitor = new LineNumberInference.LambdaImplementationVisitor(klass, ref);
         (new ClassReader(in)).accept(implVisitor, 4);
         this.descriptors.put(klass, implVisitor.descriptor());
      } catch (Throwable var14) {
         var5 = var14;
         throw var14;
      } finally {
         if(in != null) {
            if(var5 != null) {
               try {
                  in.close();
               } catch (Throwable var13) {
                  var5.addSuppressed(var13);
               }
            } else {
               in.close();
            }
         }

      }

   }

   private static String dotToSlash(String input) {
      return input.replace('.', '/');
   }

   private static String slashToDot(String input) {
      return input.replace('/', '.');
   }

   static {
      String setTmpDir = System.getProperty("jdk.internal.lambda.dumpProxyClasses");
      if(setTmpDir != null) {
         setTmpDir = setTmpDir.trim();
      }

      if(setTmpDir != null && !setTmpDir.isEmpty()) {
         File dir = new File(setTmpDir);
         if(!dir.canWrite() || !dir.canRead() || !dir.isDirectory()) {
            throw new RuntimeException("Cannot use line number inference using directory " + dir.getAbsolutePath() + " since it's not readable/writable");
         }

         logger.debug("Saving class files to {}", setTmpDir);
      } else if(Boolean.getBoolean("dse.server")) {
         logger.warn("Lambda line number inference not available");
      } else {
         logger.debug("Lambda line number inference not available");
      }

      tmpDir = setTmpDir;
   }

   public static final class Descriptor {
      final String className;
      final String sourceFilePath;
      final int line;

      Descriptor(String className, String sourceFilePath, int line) {
         this.className = className;
         this.sourceFilePath = sourceFilePath;
         this.line = line;
      }

      public int line() {
         return this.line;
      }

      public String source() {
         return this.sourceFilePath;
      }

      public String toString() {
         return "ClassDescriptor{, source='" + this.sourceFilePath + '\'' + ", line=" + this.line + '}';
      }
   }

   private static class LambdaImplementationVisitor extends ClassVisitor {
      private final Class<?> klass;
      private final LineNumberInference.LambdaClassVisitor methodReference;
      private int initLine = -1;
      private int sourceLine = -1;
      private String sourceFileName;

      LambdaImplementationVisitor(Class<?> klass, LineNumberInference.LambdaClassVisitor methodReference) {
         super(327680);
         this.klass = klass;
         this.methodReference = methodReference;
      }

      LineNumberInference.Descriptor descriptor() {
         int line = this.sourceLine != -1?this.sourceLine:this.initLine;
         return line != -1 && this.sourceFileName != null?new LineNumberInference.Descriptor(this.klass.getName(), this.sourceFileName, line):LineNumberInference.UNKNOWN_SOURCE;
      }

      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
         return name.equals("<init>")?new MethodVisitor(327680) {
            public void visitLineNumber(int line, Label label) {
               if(LambdaImplementationVisitor.this.initLine == -1) {
                  LambdaImplementationVisitor.this.initLine = line;
               }

            }
         }:(name.equals(this.methodReference.lambdaMethod) && desc.equals(this.methodReference.lambdaMethodSignature)?new MethodVisitor(327680) {
            public void visitLineNumber(int line, Label label) {
               if(LambdaImplementationVisitor.this.sourceLine == -1) {
                  LambdaImplementationVisitor.this.sourceLine = line;
               }

            }
         }:null);
      }

      public void visitSource(String fileName, String var2) {
         this.sourceFileName = fileName;
      }
   }

   private static class LambdaClassVisitor extends ClassVisitor {
      private String lambdaClass;
      private String lambdaMethod;
      private String lambdaMethodSignature;

      LambdaClassVisitor() {
         super(327680);
      }

      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
         return name.equals("<init>")?null:new MethodVisitor(327680) {
            public void visitMethodInsn(int opcode, String path, String methodRef, String methodSignature, boolean b) {
               LambdaClassVisitor.this.lambdaClass = LineNumberInference.slashToDot(path);
               LambdaClassVisitor.this.lambdaMethod = methodRef;
               LambdaClassVisitor.this.lambdaMethodSignature = methodSignature;
            }
         };
      }
   }

   private static final class SimpleClassVisitor extends ClassVisitor {
      private String className;
      private String fileName;
      private int initLine = -1;
      private int methodLine = -1;

      SimpleClassVisitor() {
         super(327680);
      }

      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
         this.className = LineNumberInference.slashToDot(name);
      }

      public LineNumberInference.Descriptor descriptor() {
         return new LineNumberInference.Descriptor(this.className, this.fileName, this.methodLine != -1?this.methodLine:this.initLine);
      }

      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
         return name.equals("<init>")?new MethodVisitor(327680) {
            public void visitLineNumber(int line, Label label) {
               SimpleClassVisitor.this.initLine = line;
            }
         }:new MethodVisitor(327680) {
            public void visitLineNumber(int line, Label label) {
               SimpleClassVisitor.this.methodLine = Math.max(SimpleClassVisitor.this.methodLine, line);
            }
         };
      }

      public void visitSource(String fileName, String var2) {
         this.fileName = fileName;
      }
   }
}
