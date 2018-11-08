package org.apache.cassandra.cql3.functions;

import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;

public interface UDFContext {
   UDTValue newArgUDTValue(String var1);

   UDTValue newArgUDTValue(int var1);

   UDTValue newReturnUDTValue();

   UDTValue newUDTValue(String var1);

   TupleValue newArgTupleValue(String var1);

   TupleValue newArgTupleValue(int var1);

   TupleValue newReturnTupleValue();

   TupleValue newTupleValue(String var1);
}
