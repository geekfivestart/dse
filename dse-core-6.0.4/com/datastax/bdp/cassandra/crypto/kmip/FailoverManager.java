package com.datastax.bdp.cassandra.crypto.kmip;

import com.cryptsoft.kmip.QueryResponse;
import com.cryptsoft.kmip.enm.ObjectType;
import com.cryptsoft.kmip.enm.Operation;
import com.cryptsoft.kmip.enm.QueryFunction;
import com.datastax.bdp.config.KmipHostOptions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverManager {
   private static final Logger logger = LoggerFactory.getLogger(FailoverManager.class);
   private static final Set<Operation> OPERATIONS;
   private static final Set<ObjectType> OBJECT_TYPES;
   private volatile List<String> errorMessages = Collections.emptyList();
   protected final String hostName;
   protected final KmipHostOptions options;
   protected final List<FailoverManager.Host> hosts;
   protected int activeHost = 0;

   protected FailoverManager(String hostName, KmipHostOptions options) throws ConfigurationException {
      this.hostName = hostName;
      this.options = options;
      List<String> hostStrings = options.getHosts();
      List<FailoverManager.Host> hostList = new ArrayList(hostStrings.size());
      Iterator var5 = hostStrings.iterator();

      while(var5.hasNext()) {
         String host = (String)var5.next();
         hostList.add(FailoverManager.Host.fromString(host));
      }

      this.hosts = ImmutableList.copyOf(hostList);
      this.validateConnections();
   }

   void validateConnections() throws ConfigurationException {
      Iterator var1 = this.hosts.iterator();

      while(var1.hasNext()) {
         FailoverManager.Host host = (FailoverManager.Host)var1.next();

         try {
            this.validateConnection(this.createConnection(host));
            logger.info("Successfully connected to KMIP host: {}", host);
            return;
         } catch (IOException var4) {
            logger.warn("Unable to connect to KMIP host: {}", host, var4);
            ++this.activeHost;
         }
      }

      throw new ConfigurationException("Unable to connect to any configured KMIP hosts");
   }

   private void validateOperations(QueryResponse response) throws IOException {
      Set<Operation> supportedOperations = Sets.newHashSet(response.getOperations());
      Iterator var3 = OPERATIONS.iterator();

      Operation required;
      do {
         if(!var3.hasNext()) {
            return;
         }

         required = (Operation)var3.next();
      } while(supportedOperations.contains(required));

      throw new IOException(String.format("The required operation %s is not supported by this host", new Object[]{required.upperName()}));
   }

   private void validateTypes(QueryResponse response) throws IOException {
      Set<ObjectType> supportedObjectTypes = Sets.newHashSet(response.getObjectTypes());
      Iterator var3 = OBJECT_TYPES.iterator();

      ObjectType required;
      do {
         if(!var3.hasNext()) {
            return;
         }

         required = (ObjectType)var3.next();
      } while(supportedObjectTypes.contains(required));

      throw new IOException(String.format("The required object type %s is not supported by this host", new Object[]{required.upperName()}));
   }

   protected void validateConnection(CloseableKmip kmip) throws IOException {
      QueryResponse response = kmip.query(new QueryFunction[]{QueryFunction.QueryOperations, QueryFunction.QueryObjects});
      this.validateOperations(response);
      this.validateTypes(response);
   }

   protected CloseableKmip createConnection(FailoverManager.Host host) throws IOException {
      return new CloseableKmip(host.getOptions(this.options.getConnectionOptions()));
   }

   public synchronized CloseableKmip getConnection() throws IOException {
      ArrayList msgs = new ArrayList(this.hosts.size());

      try {
         int i = 0;

         while(i < this.hosts.size()) {
            FailoverManager.Host host = (FailoverManager.Host)this.hosts.get(this.activeHost);

            try {
               CloseableKmip conn = this.createConnection(host);
               this.validateConnection(conn);
               CloseableKmip var5 = conn;
               return var5;
            } catch (IOException var9) {
               logger.warn("Unable to connect to active host: {}", host, var9);
               msgs.add(String.format("%s - %s", new Object[]{host, var9.getMessage()}));
               ++this.activeHost;
               this.activeHost %= this.hosts.size();
               ++i;
            }
         }

         throw new IOException(String.format("Unable to connect to any configured KMIP host %s", new Object[]{Joiner.on(", ").join(msgs)}));
      } finally {
         this.errorMessages = msgs;
      }
   }

   public List<String> getErrorMessages() {
      return this.errorMessages;
   }

   static {
      OPERATIONS = ImmutableSet.of(Operation.Locate, Operation.Get, Operation.GetAttributes, Operation.Create, Operation.Activate, Operation.AddAttribute, new Operation[0]);
      OBJECT_TYPES = ImmutableSet.of(ObjectType.SymmetricKey);
   }

   protected static class Host {
      public final String address;
      public final int port;

      public Host(String address, int port) {
         this.address = address;
         this.port = port;
      }

      public static FailoverManager.Host fromString(String host) {
         String[] parts = host.split(":");
         switch(parts.length) {
         case 1:
            return new FailoverManager.Host(parts[0], 5696);
         case 2:
            return new FailoverManager.Host(parts[0], Integer.parseInt(parts[1]));
         default:
            throw new IllegalArgumentException("Invalid host: " + host);
         }
      }

      public String toString() {
         return this.address + ":" + this.port;
      }

      public Map<String, String> getOptions(Map<String, String> opts) {
         Map<String, String> options = new HashMap(opts);
         options.put("com.cryptsoft.kmip.host", this.address);
         options.put("com.cryptsoft.kmip.port", Integer.toString(this.port));
         return options;
      }
   }
}
