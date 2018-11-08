package org.apache.cassandra.cql3.statements;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;

public class IndexPropDefs extends PropertyDefinitions {
   public static final String KW_OPTIONS = "options";
   public static final Set<String> keywords = new HashSet();
   public static final Set<String> obsoleteKeywords = new HashSet();
   public boolean isCustom;
   public String customClass;

   public IndexPropDefs() {
   }

   public void validate() throws RequestValidationException {
      this.validate(keywords, obsoleteKeywords);
      if(this.isCustom && this.customClass == null) {
         throw new InvalidRequestException("CUSTOM index requires specifiying the index class");
      } else if(!this.isCustom && this.customClass != null) {
         throw new InvalidRequestException("Cannot specify index class for a non-CUSTOM index");
      } else if(!this.isCustom && !this.properties.isEmpty()) {
         throw new InvalidRequestException("Cannot specify options for a non-CUSTOM index");
      } else if(this.getRawOptions().containsKey("class_name")) {
         throw new InvalidRequestException(String.format("Cannot specify %s as a CUSTOM option", new Object[]{"class_name"}));
      } else if(this.getRawOptions().containsKey("target")) {
         throw new InvalidRequestException(String.format("Cannot specify %s as a CUSTOM option", new Object[]{"target"}));
      }
   }

   public Map<String, String> getRawOptions() throws SyntaxException {
      Map<String, String> options = this.getMap("options");
      return options == null?Collections.emptyMap():options;
   }

   public Map<String, String> getOptions() throws SyntaxException {
      Map<String, String> options = new HashMap(this.getRawOptions());
      options.put("class_name", this.customClass);
      return options;
   }

   static {
      keywords.add("options");
   }
}
