package com.datastax.bdp.util.schema;

import com.google.common.base.Objects;
import java.util.Map;

public abstract class CqlAbstractManager {
   public CqlAbstractManager() {
   }

   void maybeReport(Map<String, InconsistentValue<Object>> inconsistencies, String paramName, Object current, Object expected) {
      if(!Objects.equal(current, expected)) {
         inconsistencies.put(paramName, new InconsistentValue(expected, current));
      }

   }
}
