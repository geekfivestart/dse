package org.apache.cassandra.auth;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.FBUtilities;

public class RoleOptions {
   private final Map<IRoleManager.Option, Object> options = new HashMap();

   public RoleOptions() {
   }

   public void setOption(IRoleManager.Option option, Object value) {
      if(this.options.containsKey(option)) {
         throw new SyntaxException(String.format("Multiple definition for property '%s'", new Object[]{option.name()}));
      } else {
         this.options.put(option, value);
      }
   }

   public boolean isEmpty() {
      return this.options.isEmpty();
   }

   public Map<IRoleManager.Option, Object> getOptions() {
      return this.options;
   }

   public Optional<Boolean> getSuperuser() {
      return Optional.fromNullable((Boolean)this.options.get(IRoleManager.Option.SUPERUSER));
   }

   public Optional<Boolean> getLogin() {
      return Optional.fromNullable((Boolean)this.options.get(IRoleManager.Option.LOGIN));
   }

   public Optional<String> getPassword() {
      return Optional.fromNullable((String)this.options.get(IRoleManager.Option.PASSWORD));
   }

   public Optional<Map<String, String>> getCustomOptions() {
      return Optional.fromNullable((Map)this.options.get(IRoleManager.Option.OPTIONS));
   }

   public void validate() {
      Iterator var1 = this.options.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<IRoleManager.Option, Object> option = (Entry)var1.next();
         if(!DatabaseDescriptor.getRoleManager().supportedOptions().contains(option.getKey())) {
            throw new InvalidRequestException(String.format("%s doesn't support %s", new Object[]{DatabaseDescriptor.getRoleManager().implementation().getClass().getName(), option.getKey()}));
         }

         switch(null.$SwitchMap$org$apache$cassandra$auth$IRoleManager$Option[((IRoleManager.Option)option.getKey()).ordinal()]) {
         case 1:
         case 2:
            if(!(option.getValue() instanceof Boolean)) {
               throw new InvalidRequestException(String.format("Invalid value for property '%s'. It must be a boolean", new Object[]{option.getKey()}));
            }
            break;
         case 3:
            if(!(option.getValue() instanceof String)) {
               throw new InvalidRequestException(String.format("Invalid value for property '%s'. It must be a string", new Object[]{option.getKey()}));
            }
            break;
         case 4:
            if(!(option.getValue() instanceof Map)) {
               throw new InvalidRequestException(String.format("Invalid value for property '%s'. It must be a map", new Object[]{option.getKey()}));
            }
         }
      }

   }

   public String toString() {
      return FBUtilities.toString(this.options);
   }
}
