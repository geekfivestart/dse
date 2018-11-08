package org.apache.cassandra.tools.nodetool.stats;

import java.util.Map;

public interface StatsHolder {
   Map<String, Object> convert2Map();
}
