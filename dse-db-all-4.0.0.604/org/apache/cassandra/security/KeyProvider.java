package org.apache.cassandra.security;

import java.io.IOException;
import java.security.Key;

public interface KeyProvider {
   Key getSecretKey(String var1) throws IOException;
}
