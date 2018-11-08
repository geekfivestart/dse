package com.datastax.bdp.cassandra.crypto;

import javax.crypto.SecretKey;

public interface IKeyProvider {
   SecretKey getSecretKey(String var1, int var2) throws KeyAccessException, KeyGenerationException;
}
