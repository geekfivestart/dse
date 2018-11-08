package com.datastax.bdp.dht;

import java.util.List;
import java.util.Map;

public interface RouterMXBean {
   List<String> getEndpoints(String var1);

   Map<String, String> getEndpointsLoad(String var1);

   void refreshEndpoints();
}
