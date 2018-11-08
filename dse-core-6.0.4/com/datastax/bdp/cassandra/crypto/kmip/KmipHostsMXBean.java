package com.datastax.bdp.cassandra.crypto.kmip;

import java.util.List;
import java.util.Map;

public interface KmipHostsMXBean {
   Map<String, List<String>> getConnectionErrors();
}
