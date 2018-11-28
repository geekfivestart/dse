package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.config.ClientConfiguration;
import com.datastax.bdp.config.ClientConfigurationBuilder;
import com.datastax.bdp.config.ClientConfigurationFactory;
import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.system.DseSystemKeyspace;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.DseConnectionUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatedKeyProvider implements IMultiKeyProvider {
   private static final Logger logger = LoggerFactory.getLogger(ReplicatedKeyProvider.class);
   private static final int HEADER_VERSION_SIZE = 1;
   private static final int UUID_HASH_SIZE = 16;
   private static final int UUID_SIZE = 16;
   private static final SecureRandom random = new SecureRandom();
   private final SystemKey systemKey;
   private final LocalFileSystemKeyProvider localKeyProvider;
   private final ConcurrentMap<String, Pair<UUID, SecretKey>> writeKeys = Maps.newConcurrentMap();
   private final ConcurrentMap<String, SecretKey> readKeys = Maps.newConcurrentMap();
   private static volatile Cluster cluster = null;
   private static volatile Session session = null;

   public ReplicatedKeyProvider(SystemKey systemKey, File localKeyFile) throws IOException {
      assert systemKey != null;

      this.systemKey = systemKey;

      LocalFileSystemKeyProvider localProvider;
      try {
         localProvider = new LocalFileSystemKeyProvider(localKeyFile);
      } catch (IOException var5) {
         localProvider = null;
      }

      this.localKeyProvider = localProvider;
   }

   private void fetchRemoteKeys() {
      TableMetadata table = DseSystemKeyspace.EncryptedKeys;
      DecoratedKey key = table.partitioner.decorateKey(ByteBufferUtil.bytes(this.systemKey.getName()));
      ReadCommand command = SinglePartitionReadCommand.create(table, FBUtilities.nowInSeconds(), key, ColumnFilter.all(table), new ClusteringIndexSliceFilter(Slices.ALL, false));
      Iterator var4 = SystemKeyspace.getHostIds().keySet().iterator();

      while(var4.hasNext()) {
         InetAddress endpoint = (InetAddress)var4.next();
         if(!Addresses.Internode.isLocalEndpoint(endpoint)) {
            ReplicatedKeyProvider.RemoteReadCallback cb = new ReplicatedKeyProvider.RemoteReadCallback(command);
            MessagingService.instance().send(command.requestTo(endpoint), cb);
            UnfilteredPartitionIterator results = cb.getResults();
            if(results != null) {
               PartitionIterator iterator = UnfilteredPartitionIterators.filter(results, FBUtilities.nowInSeconds());
               UntypedResultSet rows = QueryProcessor.resultify(String.format("SELECT * FROM %s.%s", new Object[]{"dse_system", "encrypted_keys"}), iterator);
               Iterator var10 = rows.iterator();

               while(var10.hasNext()) {
                  Row r = (Row)var10.next();
                  String readMapKey = this.getMapKey(r.getString("cipher"), r.getInt("strength"), r.getUUID("key_id"));
                  String writeMapKey = this.getMapKey(r.getString("cipher"), r.getInt("strength"));

                  try {
                     SecretKey secretKey = this.decodeKey(r.getString("key"), r.getString("cipher"));
                     this.readKeys.put(readMapKey, secretKey);
                     this.writeKeys.putIfAbsent(writeMapKey, Pair.create(r.getUUID("key_id"), secretKey));
                  } catch (IOException var15) {
                     logger.warn("Unable to decode key {}", r.getUUID("key_id"), var15);
                     throw new AssertionError(var15);
                  }
               }

               return;
            }
         }
      }

   }

   private static ReplicatedKeyProvider.RowWrapper querySingleRow(String query, boolean internalQuery) throws KeyAccessException {
      if(DseDaemon.isDaemonMode()) {
         UntypedResultSet results;
         try {
            if(internalQuery) {
               results = (UntypedResultSet)TPCUtils.blockingGet(QueryProcessor.executeOnceInternal(query, new Object[0]));
            } else {
               results = QueryProcessor.processBlocking(query, ConsistencyLevel.ONE);
            }
         } catch (RequestValidationException var4) {
            throw new KeyAccessException(var4);
         }

         return results != null && results.size() > 0?new ReplicatedKeyProvider.ResultSetRowWrapper(results.one()):null;
      } else {
         assert !internalQuery;

         try {
            ResultSet result = getCachedSession().execute(query);
            return null != result && !result.isExhausted()?new ReplicatedKeyProvider.DriverRowWrapper(result.one()):null;
         } catch (Exception var5) {
            throw new KeyAccessException(var5);
         }
      }
   }

   private static Session getCachedSession() throws Exception {
      return null != session && !session.isClosed()?session:getSynchronizedSession();
   }

   private static synchronized Session getSynchronizedSession() throws Exception {
      if(null != session && !session.isClosed()) {
         return session;
      } else {
         session = getCachedCluster().connect();
         return session;
      }
   }

   private static Cluster getCachedCluster() throws Exception {
      return null != cluster && !cluster.isClosed()?cluster:getSynchronizedCluster();
   }

   private static synchronized Cluster getSynchronizedCluster() throws Exception {
      if(null != cluster && !cluster.isClosed()) {
         return cluster;
      } else {
         String hostname = System.getProperty("dse.replicatedkeyprovider.client");
         String username = System.getProperty("dse.replicatedkeyprovider.username");
         String password = System.getProperty("dse.replicatedkeyprovider.password");
         ClientConfiguration cc = ClientConfigurationFactory.getClientConfiguration();
         InetAddress host = null == hostname?cc.getCassandraHost():InetAddress.getByName(hostname);
         ClientConfiguration config = (new ClientConfigurationBuilder(cc)).withCassandraHost(host).build();
         cluster = DseConnectionUtil.createCluster(config, username, password, (String)null);
         return cluster;
      }
   }

   private static ReplicatedKeyProvider.RowWrapper querySingleRow(String query) throws KeyAccessException {
      return querySingleRow(query, false);
   }

   private static ReplicatedKeyProvider.RowWrapper querySingleRowWithLocalRetry(String query) throws KeyAccessException {
      if(StorageService.instance.isStarting() && DseDaemon.isDaemonMode()) {
         return querySingleRow(query, true);
      } else {
         try {
            return querySingleRow(query, false);
         } catch (UnavailableException var2) {
            return querySingleRow(query, true);
         }
      }
   }

   public SecretKey getSecretKey(String cipherName, int keyStrength) throws KeyAccessException, KeyGenerationException {
      try {
         return (SecretKey)this.getWriteKey(getKeyType(cipherName), keyStrength).right;
      } catch (Throwable var4) {
         if((var4.getCause() instanceof KeyspaceNotDefinedException || var4.getCause() instanceof InvalidRequestException) && this.localKeyProvider != null) {
            return this.localKeyProvider.getSecretKey(cipherName, keyStrength);
         } else {
            throw var4;
         }
      }
   }

   private SecretKey generateNewKey(String cipherName, int keyStrength) throws IOException, NoSuchAlgorithmException {
      KeyGenerator kgen = KeyGenerator.getInstance(getKeyType(cipherName));
      kgen.init(keyStrength, random);
      return kgen.generateKey();
   }

   private String getMapKey(String cipherName, int keyStrength) {
      return cipherName + ":" + keyStrength;
   }

   private String getMapKey(String cipherName, int keyStrength, UUID keyId) {
      return this.getMapKey(cipherName, keyStrength) + ":" + keyId;
   }

   private static String getKeyType(String cipherName) {
      return cipherName.replaceAll("/.*", "");
   }

   private String encodeKey(SecretKey key) throws IOException {
      byte[] encryptedKey = this.systemKey.encrypt(key.getEncoded());
      return Base64.encodeBase64String(encryptedKey);
   }

   private SecretKey decodeKey(String encodedKey, String cipherName) throws IOException {
      byte[] encryptedKey = Base64.decodeBase64(encodedKey);
      return new SecretKeySpec(this.systemKey.decrypt(encryptedKey), getKeyType(cipherName));
   }

   private Pair<UUID, SecretKey> getWriteKey(String cipherName, int keyStrength) throws KeyGenerationException, KeyAccessException {
      String mapKey = this.getMapKey(cipherName, keyStrength);
      Pair<UUID, SecretKey> keyPair = (Pair)this.writeKeys.get(mapKey);
      if(keyPair == null && StorageService.instance.isBootstrapMode()) {
         this.fetchRemoteKeys();
         keyPair = (Pair)this.writeKeys.get(mapKey);
      }

      if(keyPair == null) {
         String query = String.format("SELECT * FROM %s.%s WHERE key_file='%s' AND cipher='%s' AND strength=%s LIMIT 1;", new Object[]{"dse_system", "encrypted_keys", this.systemKey.getName(), cipherName, Integer.valueOf(keyStrength)});
         ReplicatedKeyProvider.RowWrapper row = querySingleRowWithLocalRetry(query);
         UUID keyId;
         SecretKey key;
         if(row != null) {
            keyId = row.getUUID("key_id");

            try {
               key = this.decodeKey(row.getString("key"), cipherName);
            } catch (IOException var12) {
               throw new KeyAccessException(var12);
            }

            keyPair = Pair.create(keyId, key);
            Pair<UUID, SecretKey> previous = (Pair)this.writeKeys.putIfAbsent(mapKey, keyPair);
            this.readKeys.put(this.getMapKey(cipherName, keyStrength, keyId), key);
            if(previous != null) {
               keyPair = previous;
            }
         } else {
            keyId = UUIDGen.getTimeUUID();

            try {
               key = this.generateNewKey(cipherName, keyStrength);
            } catch (NoSuchAlgorithmException | IOException var11) {
               throw new KeyGenerationException(var11);
            }

            try {
               querySingleRow(String.format("INSERT INTO %s.%s (key_file, cipher, strength, key_id, key) VALUES ('%s', '%s', %s, %s, '%s')", new Object[]{"dse_system", "encrypted_keys", this.systemKey.getName(), cipherName, Integer.valueOf(keyStrength), keyId.toString(), this.encodeKey(key)}));
            } catch (IOException var10) {
               throw new KeyGenerationException(var10);
            }

            keyPair = Pair.create(keyId, key);
            this.writeKeys.put(mapKey, keyPair);
            this.readKeys.put(this.getMapKey(cipherName, keyStrength, keyId), key);
         }
      }

      return keyPair;
   }

   private SecretKey getReadKey(String cipherName, int keyStrength, UUID keyId) throws KeyAccessException {
      String mapKey = this.getMapKey(cipherName, keyStrength, keyId);
      SecretKey key = (SecretKey)this.readKeys.get(mapKey);
      if(key == null && StorageService.instance.isBootstrapMode()) {
         this.fetchRemoteKeys();
         key = (SecretKey)this.readKeys.get(mapKey);
      }

      if(key == null) {
         ReplicatedKeyProvider.RowWrapper row = querySingleRowWithLocalRetry(String.format("SELECT * FROM %s.%s WHERE key_file='%s' AND cipher='%s' AND strength=%s AND key_id=%s;", new Object[]{"dse_system", "encrypted_keys", this.systemKey.getName(), cipherName, Integer.valueOf(keyStrength), keyId}));
         if(row == null) {
            throw new KeyAccessException(String.format("Unable to find key for cipher=%s strength=%s id=%s", new Object[]{cipherName, Integer.valueOf(keyStrength), keyId}));
         }

         try {
            key = this.decodeKey(row.getString("key"), cipherName);
         } catch (IOException var8) {
            throw new KeyAccessException(var8);
         }

         this.readKeys.put(mapKey, key);
      }

      return key;
   }

   public SecretKey writeHeader(String cipherName, int keyStrength, ByteBuffer output) throws KeyGenerationException, KeyAccessException {
      Pair keyPair;
      try {
         keyPair = this.getWriteKey(getKeyType(cipherName), keyStrength);
      } catch (Throwable var9) {
         if((var9.getCause() instanceof KeyspaceNotDefinedException || var9.getCause() instanceof InvalidRequestException) && this.localKeyProvider != null) {
            return this.localKeyProvider.getSecretKey(cipherName, keyStrength);
         }

         throw var9;
      }

      ByteBuffer idBuffer = ByteBuffer.allocate(16);
      idBuffer.putLong(((UUID)keyPair.left).getMostSignificantBits());
      idBuffer.putLong(((UUID)keyPair.left).getLeastSignificantBits());

      MessageDigest md5;
      try {
         md5 = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException var8) {
         throw new RuntimeException(var8);
      }

      byte[] idHash = md5.digest(idBuffer.array());
      output.put((byte)0);
      output.put(idBuffer.array());
      output.put(idHash);
      return (SecretKey)keyPair.right;
   }

   public SecretKey readHeader(String cipherName, int keyStrength, ByteBuffer input) throws KeyAccessException, KeyGenerationException {
      SecretKey key = null;
      ByteBuffer inputBuffer = input.duplicate();
      byte version = inputBuffer.get();
      if(version == 0 && inputBuffer.limit() >= 32) {
         byte[] idBytes = new byte[16];
         inputBuffer.get(idBytes);
         byte[] expectedIdHash = new byte[16];
         inputBuffer.get(expectedIdHash);

         MessageDigest md5;
         try {
            md5 = MessageDigest.getInstance("MD5");
         } catch (NoSuchAlgorithmException var17) {
            throw new RuntimeException(var17);
         }

         byte[] actualIdHash = md5.digest(idBytes);
         if(ArrayUtils.isEquals(expectedIdHash, actualIdHash)) {
            ByteBuffer idBuffer = ByteBuffer.wrap(idBytes);
            long msb = idBuffer.getLong();
            long lsb = idBuffer.getLong();
            UUID keyId = new UUID(msb, lsb);
            key = this.getReadKey(getKeyType(cipherName), keyStrength, keyId);
            input.position(inputBuffer.position());
         }
      }

      if(key == null) {
         if(this.localKeyProvider == null) {
            throw new KeyAccessException(String.format("Unable to find key for cipher=%s strength=%s", new Object[]{cipherName, Integer.valueOf(keyStrength)}));
         }

         key = this.localKeyProvider.getSecretKey(cipherName, keyStrength);
      }

      return key;
   }

   public int headerLength() {
      return 33;
   }

   private class RemoteReadCallback implements MessageCallback<ReadResponse> {
      final ReadCommand command;
      volatile UnfilteredPartitionIterator results = null;
      CountDownLatch latch = new CountDownLatch(1);

      RemoteReadCallback(ReadCommand command) {
         this.command = command;
      }

      public void onResponse(Response<ReadResponse> msg) {
         this.results = FlowablePartitions.toPartitions(((ReadResponse)msg.payload()).data(this.command), DseSystemKeyspace.EncryptedKeys);
         this.latch.countDown();
      }

      public void onFailure(FailureResponse<ReadResponse> failureResponse) {
      }

      public void onTimeout(InetAddress host) {
      }

      public UnfilteredPartitionIterator getResults() {
         try {
            this.latch.await(DatabaseDescriptor.getReadRpcTimeout(), TimeUnit.MILLISECONDS);
         } catch (InterruptedException var2) {
            throw new AssertionError(var2);
         }

         return this.results;
      }
   }

   private static class DriverRowWrapper implements ReplicatedKeyProvider.RowWrapper {
      private final com.datastax.driver.core.Row row;

      private DriverRowWrapper(com.datastax.driver.core.Row row) {
         this.row = row;
      }

      public UUID getUUID(String field) {
         return this.row.getUUID(field);
      }

      public String getString(String field) {
         return this.row.getString(field);
      }
   }

   private static class ResultSetRowWrapper implements ReplicatedKeyProvider.RowWrapper {
      private final Row row;

      private ResultSetRowWrapper(Row row) {
         this.row = row;
      }

      public UUID getUUID(String field) {
         return this.row.getUUID(field);
      }

      public String getString(String field) {
         return this.row.getString(field);
      }
   }

   private interface RowWrapper {
      UUID getUUID(String var1);

      String getString(String var1);
   }
}
