package com.datastax.bdp.plugin.discovery;

import com.datastax.bdp.plugin.IPlugin;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassStreamFilter {
   private final Logger log = LoggerFactory.getLogger(ClassStreamFilter.class);
   private final Iterator<InputStream> classStreams;
   private final ClassLoader classLoader;
   private final Set<String> annotationDescriptors;
   private final Collection<String> pathPrefixes;

   public ClassStreamFilter(Iterator<InputStream> classStreams, ClassLoader classLoader, Set<Class<? extends Annotation>> annotations, Collection<String> pathPrefixes) {
      this.classStreams = classStreams;
      this.classLoader = classLoader;
      this.pathPrefixes = pathPrefixes;
      this.annotationDescriptors = this.getAnnotationDescriptors(annotations);
   }

   private Set<String> getAnnotationDescriptors(Set<Class<? extends Annotation>> annotations) {
      Set<String> a = new HashSet();
      Iterator var3 = annotations.iterator();

      while(var3.hasNext()) {
         Class c = (Class)var3.next();
         a.add("L" + c.getName().replaceAll("\\.", "/") + ";");
      }

      return a;
   }

   public Collection<Class<? extends IPlugin>> findAnnotatedClasses() throws IOException {
      ClassStreamFilter.AnnotationInspector inspector = new ClassStreamFilter.AnnotationInspector();

      while(this.classStreams.hasNext()) {
         (new ClassReader((InputStream)this.classStreams.next())).accept(inspector, 0);
      }

      return inspector.pluginClasses;
   }

   private Class getClassForName(String className) {
      try {
         return Class.forName(className, false, this.classLoader);
      } catch (ClassNotFoundException var3) {
         throw new RuntimeException(String.format("%s identified as a DSE Plugin class, but could not find class file using ClassLoader %s", new Object[]{className, this.classLoader.getClass().getName()}));
      }
   }

   private boolean checkPackageRoot(String className) {
      Iterator var2 = this.pathPrefixes.iterator();

      String prefix;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         prefix = (String)var2.next();
      } while(!className.startsWith(prefix));

      return true;
   }

   private final class AnnotationInspector extends ClassVisitor {
      private String className;
      private boolean isScoped;
      private boolean isAnnotated;
      private Set<Class<? extends IPlugin>> pluginClasses = new HashSet();

      public AnnotationInspector() {
         super(262144);
      }

      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
         this.className = name;
         this.isScoped = (access & 1) != 0;
         this.isAnnotated = false;
      }

      public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
         this.isAnnotated |= ClassStreamFilter.this.annotationDescriptors.contains(desc);
         return null;
      }

      public void visitInnerClass(String name, String outerName, String innerName, int access) {
         if(this.className.equals(name)) {
            this.isScoped = (access & 1) != 0;
            this.isScoped &= (access & 8) == 8;
         }

      }

      public void visitEnd() {
         if(this.isScoped && this.isAnnotated && ClassStreamFilter.this.checkPackageRoot(this.className)) {
            String clazzName = this.className.replaceAll("/", ".");
            Class c = ClassStreamFilter.this.getClassForName(clazzName);
            if(IPlugin.class.isAssignableFrom(c)) {
               this.pluginClasses.add(c);
            } else {
               ClassStreamFilter.this.log.warn("Annotations of class {} indicate it is a DSE Plugin but it does not implement IPlugin, this is invalid", clazzName);
            }
         }

      }

      public void visitOuterClass(String string, String string0, String string1) {
      }

      public FieldVisitor visitField(int i, String string, String string0, String string1, Object object) {
         return null;
      }

      public void visitSource(String string, String string0) {
      }

      public void visitAttribute(Attribute attribute) {
      }

      public MethodVisitor visitMethod(int i, String string, String string0, String string1, String[] string2) {
         return null;
      }
   }
}
