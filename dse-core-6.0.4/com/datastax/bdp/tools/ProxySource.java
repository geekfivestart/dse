package com.datastax.bdp.tools;

import javax.management.MalformedObjectNameException;

public interface ProxySource {
   void makeProxies(NodeJmxProxyPool var1) throws MalformedObjectNameException;
}
