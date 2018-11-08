package com.datastax.bdp.cassandra.auth;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Resources.DefaultResourceFactory;

public class DseResourceFactory extends DefaultResourceFactory {
   private static final Joiner FORWARD_SLASH_JOINER = Joiner.on('/');
   @Inject
   private static Set<DseResourceFactory.Factory> resourceFactories;

   public DseResourceFactory() {
   }

   public IResource fromName(String name) {
      Iterator var2 = resourceFactories.iterator();

      DseResourceFactory.Factory resourceFactory;
      do {
         if(!var2.hasNext()) {
            return super.fromName(name);
         }

         resourceFactory = (DseResourceFactory.Factory)var2.next();
      } while(!resourceFactory.matches(name));

      return resourceFactory.fromName(name);
   }

   public List<IResource> getExtensions(IResource resource) {
      List<IResource> extensions = new ArrayList();
      Iterator var3 = resourceFactories.iterator();

      while(var3.hasNext()) {
         DseResourceFactory.Factory resourceFactory = (DseResourceFactory.Factory)var3.next();
         IResource extension = resourceFactory.extend(resource);
         if(extension != null) {
            extensions.add(extension);
         }
      }

      return extensions;
   }

   public String makeResourceName(String... elements) {
      Iterator var2 = resourceFactories.iterator();

      DseResourceFactory.Factory resourceFactory;
      do {
         if(!var2.hasNext()) {
            return FORWARD_SLASH_JOINER.join(elements);
         }

         resourceFactory = (DseResourceFactory.Factory)var2.next();
      } while(!resourceFactory.matches(elements[0]));

      return resourceFactory.makeResourceName(elements);
   }

   public interface Factory {
      boolean matches(String var1);

      IResource fromName(String var1);

      default IResource extend(IResource resource) {
         return null;
      }

      default String makeResourceName(String... elements) {
         return DseResourceFactory.FORWARD_SLASH_JOINER.join(elements);
      }
   }
}
