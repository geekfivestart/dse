package org.apache.cassandra.cql3.functions;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;

public final class UDFByteCodeVerifier {
   public static final String JAVA_UDF_NAME = JavaUDF.class.getName().replace('.', '/');
   public static final String OBJECT_NAME = Object.class.getName().replace('.', '/');
   public static final String CTOR_SIG = "(Lorg/apache/cassandra/cql3/functions/UDFDataType;Lorg/apache/cassandra/cql3/functions/UDFContext;)V";
   private final Set<String> disallowedClasses = new HashSet();
   private final Multimap<String, String> disallowedMethodCalls = HashMultimap.create();
   private final List<String> disallowedPackages = new ArrayList();

   public UDFByteCodeVerifier() {
      this.addDisallowedMethodCall(OBJECT_NAME, "clone");
      this.addDisallowedMethodCall(OBJECT_NAME, "finalize");
      this.addDisallowedMethodCall(OBJECT_NAME, "notify");
      this.addDisallowedMethodCall(OBJECT_NAME, "notifyAll");
      this.addDisallowedMethodCall(OBJECT_NAME, "wait");
   }

   public UDFByteCodeVerifier addDisallowedClass(String clazz) {
      this.disallowedClasses.add(clazz);
      return this;
   }

   public UDFByteCodeVerifier addDisallowedMethodCall(String clazz, String method) {
      this.disallowedMethodCalls.put(clazz, method);
      return this;
   }

   public UDFByteCodeVerifier addDisallowedPackage(String pkg) {
      this.disallowedPackages.add(pkg);
      return this;
   }

   public Set<String> verify(String clsName, byte[] bytes) {
      final String clsNameSl = clsName.replace('.', '/');
      final Set<String> errors = new TreeSet();
      ClassVisitor classVisitor = new ClassVisitor(327680) {
         public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            errors.add("field declared: " + name);
            return null;
         }

         public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            if("<init>".equals(name) && "(Lorg/apache/cassandra/cql3/functions/UDFDataType;Lorg/apache/cassandra/cql3/functions/UDFContext;)V".equals(desc)) {
               if(1 != access) {
                  errors.add("constructor not public");
               }

               return new UDFByteCodeVerifier.ConstructorVisitor(errors);
            } else if("executeImpl".equals(name) && "(Lorg/apache/cassandra/cql3/functions/Arguments;)Ljava/nio/ByteBuffer;".equals(desc)) {
               if(4 != access) {
                  errors.add("executeImpl not protected");
               }

               return UDFByteCodeVerifier.this.new ExecuteImplVisitor(errors);
            } else if("executeAggregateImpl".equals(name) && "(Ljava/lang/Object;Lorg/apache/cassandra/cql3/functions/Arguments;)Ljava/lang/Object;".equals(desc)) {
               if(4 != access) {
                  errors.add("executeAggregateImpl not protected");
               }

               return UDFByteCodeVerifier.this.new ExecuteImplVisitor(errors);
            } else if("<clinit>".equals(name)) {
               errors.add("static initializer declared");
               return null;
            } else {
               errors.add("not allowed method declared: " + name + desc);
               return UDFByteCodeVerifier.this.new ExecuteImplVisitor(errors);
            }
         }

         public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            if(!UDFByteCodeVerifier.JAVA_UDF_NAME.equals(superName)) {
               errors.add("class does not extend " + JavaUDF.class.getName());
            }

            if(access != 49) {
               errors.add("class not public final");
            }

            super.visit(version, access, name, signature, superName, interfaces);
         }

         public void visitInnerClass(String name, String outerName, String innerName, int access) {
            if(clsNameSl.equals(outerName)) {
               errors.add("class declared as inner class");
            }

            super.visitInnerClass(name, outerName, innerName, access);
         }
      };
      ClassReader classReader = new ClassReader(bytes);
      classReader.accept(classVisitor, 2);
      return errors;
   }

   private static class ConstructorVisitor extends MethodVisitor {
      private final Set<String> errors;

      ConstructorVisitor(Set<String> errors) {
         super(327680);
         this.errors = errors;
      }

      public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs) {
         this.errors.add("Use of invalid method instruction in constructor");
         super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
      }

      public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
         if(183 != opcode || !UDFByteCodeVerifier.JAVA_UDF_NAME.equals(owner) || !"<init>".equals(name) || !"(Lorg/apache/cassandra/cql3/functions/UDFDataType;Lorg/apache/cassandra/cql3/functions/UDFContext;)V".equals(desc)) {
            this.errors.add("initializer declared");
         }

         super.visitMethodInsn(opcode, owner, name, desc, itf);
      }

      public void visitInsn(int opcode) {
         if(177 != opcode) {
            this.errors.add("initializer declared");
         }

         super.visitInsn(opcode);
      }
   }

   private class ExecuteImplVisitor extends MethodVisitor {
      private final Set<String> errors;

      ExecuteImplVisitor(Set<String> errors) {
         super(327680);
         this.errors = errors;
      }

      public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
         if(UDFByteCodeVerifier.this.disallowedClasses.contains(owner)) {
            this.errorDisallowed(owner, name);
         }

         Collection<String> disallowed = UDFByteCodeVerifier.this.disallowedMethodCalls.get(owner);
         if(disallowed != null && disallowed.contains(name)) {
            this.errorDisallowed(owner, name);
         }

         if(!UDFByteCodeVerifier.JAVA_UDF_NAME.equals(owner)) {
            Iterator var7 = UDFByteCodeVerifier.this.disallowedPackages.iterator();

            while(var7.hasNext()) {
               String pkg = (String)var7.next();
               if(owner.startsWith(pkg)) {
                  this.errorDisallowed(owner, name);
               }
            }
         }

         super.visitMethodInsn(opcode, owner, name, desc, itf);
      }

      private void errorDisallowed(String owner, String name) {
         this.errors.add("call to " + owner.replace('/', '.') + '.' + name + "()");
      }

      public void visitInsn(int opcode) {
         switch(opcode) {
         case 194:
         case 195:
            this.errors.add("use of synchronized");
         default:
            super.visitInsn(opcode);
         }
      }
   }
}
