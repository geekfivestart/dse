package com.datastax.bdp.util;

public interface ISharedSecretManager {
   String getOrCreateSharedSecret();

   String getSharedSecret();
}
