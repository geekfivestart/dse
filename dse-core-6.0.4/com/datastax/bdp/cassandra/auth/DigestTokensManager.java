package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.gms.VersionBarrier;
import com.datastax.bdp.system.DseSecurityKeyspace;
import com.google.inject.Singleton;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DigestTokensManager {
   private static final Logger logger = LoggerFactory.getLogger(DigestTokensManager.class);
   private final VersionBarrier versionBarrier;
   private final SelectStatement selectTokenStatement;
   private final UpdateStatement insertTokenStatement;
   private final UpdateStatement updateTokenStatement;
   private final DeleteStatement deleteTokenStatement;

   DigestTokensManager() {
      DseSecurityKeyspace.maybeConfigureKeyspace();
      this.versionBarrier = new VersionBarrier(DseVersionNotifier.VERSION_50, "DigestTokensManager will be locked until the upgrade to DSE 5.0 finishes", "DigestTokensManager unlocked after upgrade!", "DigestTokensManager requires DSE 5.0 to create/renew/use tokens");
      this.selectTokenStatement = (SelectStatement)this.prepare("SELECT password, WRITETIME(password) FROM %s.%s WHERE id = ?");
      this.insertTokenStatement = (UpdateStatement)this.prepare("INSERT INTO %s.%s (id, password) VALUES (?, ?) USING TTL ? AND TIMESTAMP ?");
      this.updateTokenStatement = (UpdateStatement)this.prepare("UPDATE %s.%s USING TTL ? AND TIMESTAMP ? SET password = ? WHERE id = ?");
      this.deleteTokenStatement = (DeleteStatement)this.prepare("DELETE FROM %s.%s WHERE id = ?");
   }

   public Optional<Pair<byte[], Long>> getPasswordById(byte[] id, ConsistencyLevel cl) {
      this.versionBarrier.check();
      logger.debug("Getting password for id: {}", Base64.encodeBase64String(id));
      ByteBuffer decomposedId = BytesType.instance.decompose(ByteBuffer.wrap(id));
      List<List<ByteBuffer>> rows = ((Rows)TPCUtils.blockingGet(this.selectTokenStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, Collections.singletonList(decomposedId)), System.nanoTime()))).result.rows;
      if(rows.size() > 0) {
         List<ByteBuffer> row = (List)rows.get(0);
         if(row.size() > 0) {
            byte[] password = null;
            long timestamp = -1L;
            ByteBuffer decomposed = (ByteBuffer)row.get(0);
            if(decomposed != null) {
               password = ByteBufferUtil.getArray((ByteBuffer)BytesType.instance.compose(decomposed));
            }

            decomposed = (ByteBuffer)row.get(1);
            if(decomposed != null) {
               timestamp = ((Date)TimestampType.instance.compose(decomposed)).getTime();
            }

            return Optional.of(Pair.create(password, Long.valueOf(timestamp)));
         }
      }

      return Optional.empty();
   }

   public void createToken(byte[] id, byte[] password, Duration maxLifetime, ConsistencyLevel cl, long ts) {
      this.versionBarrier.check();
      logger.debug("Creating token for id: {}", Base64.encodeBase64String(id));
      ByteBuffer decomposedId = BytesType.instance.decompose(ByteBuffer.wrap(id));
      ByteBuffer decomposedPassword = BytesType.instance.decompose(ByteBuffer.wrap(password));
      ByteBuffer decomposedTTL = Int32Type.instance.decompose(Integer.valueOf((int)maxLifetime.getSeconds()));
      ByteBuffer decomposedTimestamp = TimestampType.instance.decompose(new Date(ts));
      TPCUtils.blockingGet(this.insertTokenStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, Arrays.asList(new ByteBuffer[]{decomposedId, decomposedPassword, decomposedTTL, decomposedTimestamp})), System.nanoTime()));
   }

   public void updateToken(byte[] id, byte[] password, Duration maxLifetime, ConsistencyLevel cl, long ts) {
      this.versionBarrier.check();
      logger.debug("Updating token for id: {}", Base64.encodeBase64String(id));
      ByteBuffer decomposedId = BytesType.instance.decompose(ByteBuffer.wrap(id));
      ByteBuffer decomposedPassword = BytesType.instance.decompose(ByteBuffer.wrap(password));
      ByteBuffer decomposedTTL = Int32Type.instance.decompose(Integer.valueOf((int)maxLifetime.getSeconds()));
      ByteBuffer decomposedTimestamp = TimestampType.instance.decompose(new Date(ts));
      TPCUtils.blockingGet(this.updateTokenStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, Arrays.asList(new ByteBuffer[]{decomposedTTL, decomposedTimestamp, decomposedPassword, decomposedId})), System.nanoTime()));
   }

   public void deleteTokenById(byte[] id, ConsistencyLevel cl) {
      this.versionBarrier.check();
      logger.debug("Deleting token for id: {}", Base64.encodeBase64String(id));
      ByteBuffer decomposedId = BytesType.instance.decompose(ByteBuffer.wrap(id));
      TPCUtils.blockingGet(this.deleteTokenStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, Collections.singletonList(decomposedId)), System.nanoTime()));
   }

   private CQLStatement prepare(String cql) {
      String query = String.format(cql, new Object[]{"dse_security", "digest_tokens"});
      return StatementUtils.prepareStatementBlocking(query, QueryState.forInternalCalls(), "Error preparing \"" + query + "\"");
   }
}
