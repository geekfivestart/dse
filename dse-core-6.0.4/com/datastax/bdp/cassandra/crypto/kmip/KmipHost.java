package com.datastax.bdp.cassandra.crypto.kmip;

import com.cryptsoft.kmip.Att;
import com.cryptsoft.kmip.CreateResponse;
import com.cryptsoft.kmip.GetAttributesResponse;
import com.cryptsoft.kmip.GetResponse;
import com.cryptsoft.kmip.KeyBlock;
import com.cryptsoft.kmip.Kmip;
import com.cryptsoft.kmip.KmipException;
import com.cryptsoft.kmip.LocateResponse;
import com.cryptsoft.kmip.Name;
import com.cryptsoft.kmip.ResponseMessage;
import com.cryptsoft.kmip.TTLV;
import com.cryptsoft.kmip.TemplateAttribute;
import com.cryptsoft.kmip.enm.BlockCipherMode;
import com.cryptsoft.kmip.enm.CryptographicAlgorithm;
import com.cryptsoft.kmip.enm.CryptographicUsageMask;
import com.cryptsoft.kmip.enm.Enum;
import com.cryptsoft.kmip.enm.HashingAlgorithm;
import com.cryptsoft.kmip.enm.KeyRoleType;
import com.cryptsoft.kmip.enm.ObjectType;
import com.cryptsoft.kmip.enm.PaddingMethod;
import com.cryptsoft.kmip.enm.ResultReason;
import com.cryptsoft.kmip.enm.State;
import com.cryptsoft.kmip.enm.Type;
import com.datastax.bdp.cassandra.crypto.KeyAccessException;
import com.datastax.bdp.cassandra.crypto.KeyGenerationException;
import com.datastax.bdp.config.KmipHostOptions;
import com.datastax.bdp.server.SystemInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import javax.crypto.SecretKey;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;

public class KmipHost {
   private static final String KEY_NAMESPACE_ATTR = "x-key-namespace";
   private static final String DSE_VERSION_ATTR = "x-dse-version";
   private static final Att[] NO_ATTRS = new Att[0];
   private final String hostName;
   private final FailoverManager failoverManager;
   protected final KmipHost.KeyCache<String> attrCache;
   protected final KmipHost.KeyCache<String> idCache;

   public KmipHost(String hostName, KmipHostOptions options) throws IOException, ConfigurationException {
      this.hostName = hostName;
      this.attrCache = new KmipHost.KeyCache(options.key_cache_millis);
      this.idCache = new KmipHost.KeyCache(options.key_cache_millis);
      this.failoverManager = this.createFailoverManager(hostName, options);
   }

   FailoverManager createFailoverManager(String hostName, KmipHostOptions options) throws ConfigurationException {
      return new FailoverManager(hostName, options);
   }

   @VisibleForTesting
   public CloseableKmip getConnection() throws IOException {
      return this.failoverManager.getConnection();
   }

   public String getHostName() {
      return this.hostName;
   }

   public List<String> getErrorMessages() {
      return this.failoverManager.getErrorMessages();
   }

   @VisibleForTesting
   public List<String> getMatchingKeyIds(String cipher, int keyStrength, KmipHost.Options options, Kmip kmip) throws IOException {
      KmipHost.CipherInfo cipherInfo = new KmipHost.CipherInfo(cipher);
      int idx = 0;
      Att[] attrs = new Att[6 + (options.namespace != null?1:0)];
      idx = idx + 1;
      attrs[idx] = Att.cryptographicAlgorithm(cipherInfo.getCryptographicAlgorithm());
      attrs[idx++] = Att.cryptographicLength(keyStrength);
      attrs[idx++] = Att.objectType(ObjectType.SymmetricKey);
      attrs[idx++] = Att.cryptographicUsageMask(new CryptographicUsageMask[]{CryptographicUsageMask.Encrypt, CryptographicUsageMask.Decrypt});
      attrs[idx++] = Att.state(State.Active);
      attrs[idx++] = Att.cryptographicParameters(cipherInfo.getBlockCipherMode(), cipherInfo.getPaddingMethod(), (HashingAlgorithm)null, (KeyRoleType)null);
      if(options.namespace != null) {
         attrs[idx++] = Att.custom("x-key-namespace", options.namespace);
      }

      assert attrs.length == idx;

      LocateResponse locateResponse = kmip.locate(attrs);
      return locateResponse.getUniqueIdentifiers();
   }

   @VisibleForTesting
   String createKey(String cipher, int keyStrength, KmipHost.Options options, Kmip kmip) throws IOException {
      KmipHost.CipherInfo cipherInfo = new KmipHost.CipherInfo(cipher);
      CryptographicAlgorithm algorithm = cipherInfo.getCryptographicAlgorithm();
      int idx = 0;
      Att[] attrs = new Att[5 + (options.namespace != null?1:0)];
      idx = idx + 1;
      attrs[idx] = Att.cryptographicAlgorithm(algorithm);
      attrs[idx++] = Att.cryptographicLength(keyStrength);
      attrs[idx++] = Att.cryptographicUsageMask(new CryptographicUsageMask[]{CryptographicUsageMask.Encrypt, CryptographicUsageMask.Decrypt});
      attrs[idx++] = Att.cryptographicParameters(cipherInfo.getBlockCipherMode(), cipherInfo.getPaddingMethod(), (HashingAlgorithm)null, (KeyRoleType)null);
      attrs[idx++] = Att.custom("x-dse-version", SystemInfo.getReleaseVersion("dse_version"));
      if(options.namespace != null) {
         attrs[idx++] = Att.custom("x-key-namespace", options.namespace);
      }

      assert attrs.length == idx;

      TemplateAttribute tmplAttrs = options.template == null?Att.ta(attrs):Att.ta(new Name[]{new Name(options.template)}, attrs);
      CreateResponse createResponse = kmip.create(ObjectType.SymmetricKey, tmplAttrs);
      String id = createResponse.getUniqueIdentifier();
      kmip.activate(id);
      return id;
   }

   public String createKey(String cipher, int keyStrength, KmipHost.Options options) throws IOException {
      CloseableKmip kmip = this.getConnection();
      Throwable var5 = null;

      String var6;
      try {
         var6 = this.createKey(cipher, keyStrength, options, kmip);
      } catch (Throwable var15) {
         var5 = var15;
         throw var15;
      } finally {
         if(kmip != null) {
            if(var5 != null) {
               try {
                  kmip.close();
               } catch (Throwable var14) {
                  var5.addSuppressed(var14);
               }
            } else {
               kmip.close();
            }
         }

      }

      return var6;
   }

   public List<KmipHost.KeyAttrs> listAll(KmipHost.Options options) throws IOException, KeyAccessException {
      CloseableKmip kmip = this.getConnection();
      Throwable var3 = null;

      List var6;
      try {
         Att[] attrs = options.namespace != null?new Att[]{Att.custom("x-key-namespace", options.namespace)}:NO_ATTRS;
         LocateResponse response = kmip.locate(attrs);
         var6 = this.getKeyAttrs((List)response.getUniqueIdentifiers(), kmip);
      } catch (Throwable var15) {
         var3 = var15;
         throw var15;
      } finally {
         if(kmip != null) {
            if(var3 != null) {
               try {
                  kmip.close();
               } catch (Throwable var14) {
                  var3.addSuppressed(var14);
               }
            } else {
               kmip.close();
            }
         }

      }

      return var6;
   }

   public void stopKey(String keyId) throws IOException {
      this.stopKey(keyId, new Date());
   }

   public void stopKey(String keyId, Date when) throws IOException {
      CloseableKmip kmip = this.getConnection();
      Throwable var4 = null;

      try {
         kmip.addAttribute(keyId, Att.protectStopDate(when));
      } catch (Throwable var13) {
         var4 = var13;
         throw var13;
      } finally {
         if(kmip != null) {
            if(var4 != null) {
               try {
                  kmip.close();
               } catch (Throwable var12) {
                  var4.addSuppressed(var12);
               }
            } else {
               kmip.close();
            }
         }

      }

   }

   List<String> sortAndFilterKeys(List<String> ids, Kmip kmip) throws KeyAccessException {
      List<KmipHost.KeyAttrs> keyAttrs = this.getKeyAttrs(ids, kmip);
      final Date now = new Date();
      keyAttrs = Lists.newArrayList(Iterables.filter(keyAttrs, new Predicate<KmipHost.KeyAttrs>() {
         public boolean apply(KmipHost.KeyAttrs input) {
            return input.protectStopDate == null || !input.protectStopDate.before(now);
         }
      }));
      Collections.sort(keyAttrs, new Comparator<KmipHost.KeyAttrs>() {
         private int nullableDateCompare(Date d1, Date d2) {
            return d1 == null && d2 == null?0:(d1 != null && d2 != null?d1.compareTo(d2):(d1 == null?1:-1));
         }

         public int compare(KmipHost.KeyAttrs o1, KmipHost.KeyAttrs o2) {
            int cmp = this.nullableDateCompare(o1.protectStopDate, o2.protectStopDate);
            return cmp != 0?cmp:this.nullableDateCompare(o1.activationDate, o2.activationDate);
         }
      });
      return Lists.newArrayList(Iterables.transform(keyAttrs, new Function<KmipHost.KeyAttrs, String>() {
         public String apply(KmipHost.KeyAttrs input) {
            return input.id;
         }
      }));
   }

   @VisibleForTesting
   protected String locateKey(String cipherName, int keyStrength, KmipHost.Options options, Kmip kmip) throws KeyAccessException {
      try {
         List<String> keyIds = this.getMatchingKeyIds(cipherName, keyStrength, options, kmip);
         if(keyIds.size() > 0) {
            keyIds = this.sortAndFilterKeys(keyIds, kmip);
            return keyIds.size() > 0?(String)keyIds.get(0):null;
         } else {
            return null;
         }
      } catch (IOException var6) {
         throw new KeyAccessException(var6);
      }
   }

   KmipHost.KeyAttrs getKeyAttrs(String keyId, Kmip kmip) throws KeyAccessException {
      try {
         GetAttributesResponse response = kmip.getAttributes(keyId, new String[0]);

         assert response.getUniqueIdentifier().equals(keyId);

         return new KmipHost.KeyAttrs(keyId, response.getAtts());
      } catch (IOException var4) {
         throw new KeyAccessException(var4);
      }
   }

   List<KmipHost.KeyAttrs> getKeyAttrs(List<String> keyIds, Kmip kmip) throws KeyAccessException {
      if(keyIds.isEmpty()) {
         return Collections.emptyList();
      } else {
         try {
            kmip.batchStart();
            Iterator var3 = keyIds.iterator();

            while(var3.hasNext()) {
               String id = (String)var3.next();
               kmip.getAttributes(id, new String[0]);
            }

            ResponseMessage batchResponse = kmip.batchSend();
            List<KmipHost.KeyAttrs> attrsList = new ArrayList(keyIds.size());

            for(int i = 0; i < keyIds.size(); ++i) {
               String id = (String)keyIds.get(i);
               GetAttributesResponse response = batchResponse.getGetAttributesResponse(i);

               assert response.getUniqueIdentifier().equals(id);

               attrsList.add(new KmipHost.KeyAttrs(id, response.getAtts()));
            }

            return attrsList;
         } catch (IOException var8) {
            throw new KeyAccessException(var8);
         }
      }
   }

   @VisibleForTesting
   protected KmipHost.Key getKey(String id, Kmip kmip) throws KeyAccessException {
      try {
         kmip.batchStart();
         kmip.get(id);
         kmip.getAttributes(id, new String[0]);
         ResponseMessage response = kmip.batchSend();
         GetResponse getResponse = response.getGetResponse(0);
         GetAttributesResponse attrsResponse = response.getGetAttributesResponse(1);
         if(getResponse.getObjectType() != ObjectType.SymmetricKey) {
            throw new KeyAccessException(String.format("Unexpected key type for %s. Expected %s, got %s", new Object[]{id, ObjectType.SymmetricKey.upperName(), getResponse.getObjectType().upperName()}));
         } else {
            KeyBlock keyBlock = getResponse.getKeyBlock();
            KmipHost.KeyAttrs keyAttrs = new KmipHost.KeyAttrs(id, attrsResponse.getAtts());
            return new KmipHost.Key(keyAttrs.cipherInfo.asJavaCipher(), keyAttrs.strength, id, keyBlock.getSymmetricKey());
         }
      } catch (KmipException var8) {
         if(var8.getResultReason() == ResultReason.ItemNotFound) {
            throw new KeyAccessException(String.format("Key '%s' not found on kmip server", new Object[]{id}));
         } else {
            throw var8;
         }
      } catch (IOException var9) {
         throw new KeyAccessException(var9);
      }
   }

   private static void validateKey(KmipHost.Key key, String id, String cipher, int strength) throws KeyAccessException {
      if(!cipher.equalsIgnoreCase(key.cipher)) {
         throw new KeyAccessException(String.format("Unexpected cipher string for key %s. Expected %s, got %s", new Object[]{id, cipher, key.cipher}));
      } else if(strength != key.strength) {
         throw new KeyAccessException(String.format("Unexpected key strength for key %s. Expected %s, got %s", new Object[]{id, Integer.valueOf(strength), Integer.valueOf(key.strength)}));
      }
   }

   static String getCacheKey(String cipher, int strength, KmipHost.Options options) {
      return String.format("%s::%s::%s::%s", new Object[]{cipher, Integer.valueOf(strength), options.template, options.namespace});
   }

   public KmipHost.Key getOrCreateByAttrs(String cipher, int strength, KmipHost.Options options) throws KeyAccessException, KeyGenerationException {
      String cacheKey = getCacheKey(cipher, strength, options);
      KmipHost.Key key = this.attrCache.get(cacheKey);
      if(key != null) {
         return key;
      } else {
         synchronized(this) {
            key = this.attrCache.get(cacheKey);
            if(key != null) {
               return key;
            } else {
               KmipHost.Key var12;
               try {
                  CloseableKmip kmip = this.getConnection();
                  Throwable var8 = null;

                  try {
                     String keyId = this.locateKey(cipher, strength, options, kmip);
                     if(keyId == null) {
                        try {
                           keyId = this.createKey(cipher, strength, options, kmip);
                        } catch (IOException var25) {
                           throw new KeyGenerationException(var25);
                        }
                     }

                     key = this.getKey(keyId, kmip);
                     validateKey(key, keyId, cipher, strength);
                     long now = System.currentTimeMillis();
                     this.attrCache.put(cacheKey, key, now);
                     this.idCache.put(keyId, key, now);
                     var12 = key;
                  } catch (Throwable var26) {
                     var8 = var26;
                     throw var26;
                  } finally {
                     if(kmip != null) {
                        if(var8 != null) {
                           try {
                              kmip.close();
                           } catch (Throwable var24) {
                              var8.addSuppressed(var24);
                           }
                        } else {
                           kmip.close();
                        }
                     }

                  }
               } catch (IOException var28) {
                  throw new KeyAccessException(var28);
               }

               return var12;
            }
         }
      }
   }

   public KmipHost.Key getById(String id) throws KeyAccessException {
      KmipHost.Key key = this.idCache.get(id);
      if(key != null) {
         return key;
      } else {
         synchronized(this) {
            key = this.idCache.get(id);
            if(key != null) {
               return key;
            } else {
               KmipHost.Key var6;
               try {
                  CloseableKmip kmip = this.getConnection();
                  Throwable var5 = null;

                  try {
                     key = this.getKey(id, kmip);
                     this.idCache.put(id, key, System.currentTimeMillis());
                     var6 = key;
                  } catch (Throwable var18) {
                     var5 = var18;
                     throw var18;
                  } finally {
                     if(kmip != null) {
                        if(var5 != null) {
                           try {
                              kmip.close();
                           } catch (Throwable var17) {
                              var5.addSuppressed(var17);
                           }
                        } else {
                           kmip.close();
                        }
                     }

                  }
               } catch (IOException var20) {
                  throw new KeyAccessException(var20);
               }

               return var6;
            }
         }
      }
   }

   public static class KeyCache<KeyType> {
      private final long cacheTimeMillis;
      private final ConcurrentMap<KeyType, Pair<KmipHost.Key, Long>> cache = Maps.newConcurrentMap();

      public KeyCache(long cacheTimeMillis) {
         this.cacheTimeMillis = cacheTimeMillis;
      }

      public KmipHost.Key get(KeyType k) {
         Pair<KmipHost.Key, Long> cached = (Pair)this.cache.get(k);
         if(cached != null) {
            long elapsed = System.currentTimeMillis() - ((Long)cached.right).longValue();
            if(elapsed <= this.cacheTimeMillis) {
               return (KmipHost.Key)cached.left;
            }

            this.cache.remove(k, cached);
         }

         return null;
      }

      public void put(KeyType k, KmipHost.Key v) {
         this.put(k, v, System.currentTimeMillis());
      }

      public void put(KeyType k, KmipHost.Key v, long time) {
         this.cache.put(k, Pair.create(v, Long.valueOf(time)));
      }
   }

   public static class CipherInfo {
      public final String cipherName;
      public final String cipherMode;
      public final String paddingMethod;

      public CipherInfo(String cipherString) {
         String[] parts = cipherString.split("/");
         if(parts.length != 1 && parts.length != 3) {
            throw new IllegalArgumentException("Malformed cipher string: " + cipherString);
         } else {
            this.cipherName = parts[0];
            this.cipherMode = parts.length == 3?parts[1]:null;
            this.paddingMethod = parts.length == 3?parts[2].replaceAll("Padding$", ""):null;
         }
      }

      public CipherInfo(String cipherName, String cipherMode, String paddingMethod) {
         this.cipherName = cipherName;
         this.cipherMode = cipherMode;
         this.paddingMethod = paddingMethod;
      }

      public CryptographicAlgorithm getCryptographicAlgorithm() {
         try {
            return (CryptographicAlgorithm)CryptographicAlgorithm.fromName(CryptographicAlgorithm.class, this.cipherName);
         } catch (Exception var2) {
            throw new IllegalArgumentException("The algorithm '" + this.cipherName + "' cannot be used with a KMIP key provider");
         }
      }

      public BlockCipherMode getBlockCipherMode() {
         try {
            return this.cipherMode == null?null:(BlockCipherMode)BlockCipherMode.fromName(BlockCipherMode.class, this.cipherMode);
         } catch (Exception var2) {
            throw new IllegalArgumentException("The block cipher mode '" + this.cipherMode + "' cannot be used with a KMIP key provider");
         }
      }

      public PaddingMethod getPaddingMethod() {
         try {
            return this.paddingMethod == null?null:(PaddingMethod)PaddingMethod.fromName(PaddingMethod.class, this.paddingMethod);
         } catch (Exception var2) {
            throw new IllegalArgumentException("The padding method '" + this.paddingMethod + "' cannot be used with a KMIP key provider");
         }
      }

      public boolean hasParameters() {
         return this.cipherMode != null && this.paddingMethod != null;
      }

      public String asJavaCipher() {
         return this.hasParameters()?String.format("%s/%s/%sPadding", new Object[]{this.cipherName, this.cipherMode, this.paddingMethod}):this.cipherName;
      }

      public String toString() {
         return this.cipherMode != null && this.paddingMethod != null?this.cipherName + "/" + this.cipherMode + "/" + this.paddingMethod:this.cipherName;
      }
   }

   public static class KeyAttrs {
      public final String id;
      public final String name;
      public final String namespace;
      public final KmipHost.CipherInfo cipherInfo;
      public final int strength;
      public final Date activationDate;
      public final Date creationDate;
      public final Date protectStopDate;
      public final State state;

      public KeyAttrs(String id, List<Att> attrs) throws KeyAccessException {
         this.id = id;
         this.cipherInfo = this.createCipherInfo(attrs);
         Integer strength = null;
         State state = null;
         String name = null;
         String namespace = null;
         Iterator var7 = attrs.iterator();

         while(true) {
            while(var7.hasNext()) {
               Att attr = (Att)var7.next();
               String attrName = attr.getAttributeName();
               if(attrName.equals("Cryptographic Length")) {
                  strength = Integer.valueOf(attr.getAttributeValue().getValueInt());
               } else if(attrName.equals("Name")) {
                  Iterator var10 = attr.getAttributeValue().split().iterator();

                  while(var10.hasNext()) {
                     TTLV part = (TTLV)var10.next();
                     if(part.getType().equals(Type.TextString)) {
                        name = part.getValueUtf8();
                        break;
                     }
                  }
               } else if(attrName.equals("State")) {
                  state = (State)attr.getAttributeValue().getValueEnumeration();
               } else if(attrName.equals("x-key-namespace")) {
                  namespace = attr.getAttributeValue().getValueUtf8();
               }
            }

            if(strength == null) {
               throw new KeyAccessException("Key strength not included in attributes");
            }

            this.strength = strength.intValue();
            this.name = name;
            this.namespace = namespace;
            this.state = state;
            this.activationDate = this.getDate("Activation Date", attrs);
            this.creationDate = this.getDate("Original Creation Date", attrs);
            this.protectStopDate = this.getDate("Protect Stop Date", attrs);
            return;
         }
      }

      private KmipHost.CipherInfo createCipherInfo(List<Att> attrs) throws KeyAccessException {
         String cipherName = null;
         String cipherMode = null;
         String paddingMethod = null;
         Iterator var5 = attrs.iterator();

         while(var5.hasNext()) {
            Att attr = (Att)var5.next();
            String attrName = attr.getAttributeName();
            if(attrName.equals("Cryptographic Algorithm")) {
               Enum e = attr.getAttributeValue().getValueEnumeration();

               assert e instanceof CryptographicAlgorithm;

               CryptographicAlgorithm algorithm = (CryptographicAlgorithm)e;
               cipherName = algorithm.upperName();
            } else if(attrName.equals("Cryptographic Parameters")) {
               Object o = attr.getAttributeValue().getValueObject();

               assert o instanceof Iterable;

               List<TTLV> values = (List)o;
               Iterator var10 = values.iterator();

               while(var10.hasNext()) {
                  TTLV value = (TTLV)var10.next();
                  Enum e = value.getValueEnumeration();
                  if(e instanceof BlockCipherMode) {
                     BlockCipherMode mode = (BlockCipherMode)e;
                     cipherMode = mode.upperName();
                  } else if(e instanceof PaddingMethod) {
                     PaddingMethod method = (PaddingMethod)e;
                     paddingMethod = method.upperName();
                  }
               }
            }

            if(cipherName != null && cipherMode != null && paddingMethod != null) {
               break;
            }
         }

         if(cipherName == null) {
            throw new KeyAccessException("Cipher name not included in attributes");
         } else {
            assert cipherMode == null && paddingMethod == null || cipherMode != null && paddingMethod != null;

            return new KmipHost.CipherInfo(cipherName, cipherMode, paddingMethod);
         }
      }

      private Date getDate(String name, List<Att> attrs) {
         Iterator var3 = attrs.iterator();

         Att attr;
         do {
            if(!var3.hasNext()) {
               return null;
            }

            attr = (Att)var3.next();
         } while(!attr.getAttributeName().equals(name));

         return attr.getAttributeValue().getValueDate();
      }
   }

   public static class Options {
      public static final KmipHost.Options NONE = new KmipHost.Options((String)null, (String)null);
      public final String template;
      public final String namespace;

      public Options(String template, String namespace) {
         this.template = template;
         this.namespace = namespace;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            KmipHost.Options options = (KmipHost.Options)o;
            if(this.namespace != null) {
               if(!this.namespace.equals(options.namespace)) {
                  return false;
               }
            } else if(options.namespace != null) {
               return false;
            }

            if(this.template != null) {
               if(this.template.equals(options.template)) {
                  return true;
               }
            } else if(options.template == null) {
               return true;
            }

            return false;
         } else {
            return false;
         }
      }

      public int hashCode() {
         int result = this.template != null?this.template.hashCode():0;
         result = 31 * result + (this.namespace != null?this.namespace.hashCode():0);
         return result;
      }

      public String toString() {
         return "KmipHost.Options{template='" + this.template + '\'' + ", namespace='" + this.namespace + '\'' + '}';
      }
   }

   public static class Key {
      public final String cipher;
      public final int strength;
      public final String id;
      public final SecretKey key;

      Key(String cipher, int strength, String id, SecretKey key) {
         this.cipher = cipher;
         this.strength = strength;
         this.id = id;
         this.key = key;
      }

      public String toString() {
         return "KmipHost.Key{cipher='" + this.cipher + '\'' + ", strength=" + this.strength + ", id='" + this.id + '\'' + '}';
      }
   }
}
