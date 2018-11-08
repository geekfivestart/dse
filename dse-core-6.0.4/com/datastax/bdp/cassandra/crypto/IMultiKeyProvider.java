package com.datastax.bdp.cassandra.crypto;

import java.nio.ByteBuffer;
import javax.crypto.SecretKey;

public interface IMultiKeyProvider extends IKeyProvider {
   SecretKey writeHeader(String var1, int var2, ByteBuffer var3) throws KeyAccessException, KeyGenerationException;

   SecretKey readHeader(String var1, int var2, ByteBuffer var3) throws KeyAccessException, KeyGenerationException;

   int headerLength();
}
