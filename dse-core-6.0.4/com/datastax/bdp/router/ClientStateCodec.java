package com.datastax.bdp.router;

import io.netty.buffer.ByteBuf;
import java.net.InetSocketAddress;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.ProtocolVersion;

public class ClientStateCodec implements CBCodec<ClientState> {
   public static final CBCodec<ClientState> INSTANCE = new ClientStateCodec();
   private final long IS_INTERNAL = 1L;
   private final long HAS_USERNAME = 4L;
   private final long HAS_KEYSPACE = 8L;

   public ClientStateCodec() {
   }

   public ClientState decode(ByteBuf body, ProtocolVersion version) {
      long flags = body.readLong();
      ClientState clientState;
      if((flags & 1L) != 0L) {
         clientState = ClientState.forInternalCalls();
      } else {
         InetSocketAddress address = this.readInet(body);
         clientState = ClientState.forExternalCalls(address, (Connection)null);
      }

      if((flags & 8L) != 0L) {
         clientState.setKeyspace(CBUtil.readString(body));
      }

      if((flags & 4L) != 0L) {
         AuthenticatedUser user = new AuthenticatedUser(CBUtil.readString(body));
         TPCUtils.blockingGet(clientState.login(user));
      }

      return clientState;
   }

   public void encode(ClientState clientState, ByteBuf dest, ProtocolVersion version) {
      long flags = 0L;
      if(clientState.isInternal) {
         flags |= 1L;
      }

      if(clientState.getRawKeyspace() != null) {
         flags |= 8L;
      }

      if(clientState.getUser() != null && !clientState.getUser().isAnonymous()) {
         flags |= 4L;
      }

      dest.writeLong(flags);
      if(!clientState.isInternal) {
         this.writeInet(clientState.getRemoteAddress(), dest);
      }

      if(clientState.getRawKeyspace() != null) {
         CBUtil.writeString(clientState.getRawKeyspace(), dest);
      }

      if(clientState.getUser() != null && !clientState.getUser().isAnonymous()) {
         CBUtil.writeString(clientState.getUser().getName(), dest);
      }

   }

   public int encodedSize(ClientState clientState, ProtocolVersion version) {
      int size = 8;
      if(!clientState.isInternal) {
         size += this.sizeOfInet(clientState.getRemoteAddress());
      }

      if(clientState.getRawKeyspace() != null) {
         size += CBUtil.sizeOfString(clientState.getRawKeyspace());
      }

      if(clientState.getUser() != null && !clientState.getUser().isAnonymous()) {
         size += CBUtil.sizeOfString(clientState.getUser().getName());
      }

      return size;
   }

   private InetSocketAddress readInet(ByteBuf buf) {
      boolean resolved = buf.readBoolean();
      if(resolved) {
         return CBUtil.readInet(buf);
      } else {
         String hostName = CBUtil.readString(buf);
         int port = buf.readInt();
         return InetSocketAddress.createUnresolved(hostName, port);
      }
   }

   private void writeInet(InetSocketAddress addr, ByteBuf buf) {
      if(addr.isUnresolved()) {
         buf.writeBoolean(false);
         CBUtil.writeString(addr.getHostName(), buf);
         buf.writeInt(addr.getPort());
      } else {
         buf.writeBoolean(true);
         CBUtil.writeInet(addr, buf);
      }

   }

   private int sizeOfInet(InetSocketAddress addr) {
      int size = 1;
      if(addr.isUnresolved()) {
         size = size + CBUtil.sizeOfString(addr.getHostName());
         size += 4;
      } else {
         size = size + CBUtil.sizeOfInet(addr);
      }

      return size;
   }
}
