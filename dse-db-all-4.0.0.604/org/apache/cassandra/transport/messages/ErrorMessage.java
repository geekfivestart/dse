package org.apache.cassandra.transport.messages;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CodecException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ClientWriteException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.exceptions.TransportException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.ServerError;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorMessage extends Message.Response {
   private static final Logger logger = LoggerFactory.getLogger(ErrorMessage.class);
   public static final Message.Codec<ErrorMessage> codec = new Message.Codec<ErrorMessage>() {
      public ErrorMessage decode(ByteBuf body, ProtocolVersion version) {
         ExceptionCode code = ExceptionCode.fromValue(body.readInt());
         String msg = CBUtil.readString(body);
         TransportException te = null;
         ConsistencyLevel cl;
         int received;
         int blockFor;
         switch(null.$SwitchMap$org$apache$cassandra$exceptions$ExceptionCode[code.ordinal()]) {
         case 1:
            te = new ServerError(msg);
            break;
         case 2:
            te = new ProtocolException(msg);
            break;
         case 3:
            te = new AuthenticationException(msg);
            break;
         case 4:
            cl = CBUtil.readConsistencyLevel(body);
            received = body.readInt();
            blockFor = body.readInt();
            te = new UnavailableException(cl, received, blockFor);
            break;
         case 5:
            te = new OverloadedException(msg);
            break;
         case 6:
            te = new IsBootstrappingException();
            break;
         case 7:
            te = new TruncateException(msg);
            break;
         case 8:
         case 9:
            cl = CBUtil.readConsistencyLevel(body);
            received = body.readInt();
            blockFor = body.readInt();
            int failure = body.readInt();
            Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = new HashMap();
            if(version.isGreaterOrEqualTo(ProtocolVersion.V5)) {
               for(int i = 0; i < failure; ++i) {
                  InetAddress endpoint = CBUtil.readInetAddr(body);
                  RequestFailureReason failureReason = RequestFailureReason.fromCode(body.readUnsignedShort());
                  failureReasonByEndpoint.put(endpoint, failureReason);
               }
            }

            if(code == ExceptionCode.WRITE_FAILURE) {
               WriteType writeTypex = (WriteType)Enum.valueOf(WriteType.class, CBUtil.readString(body));
               te = new WriteFailureException(cl, received, blockFor, writeTypex, failureReasonByEndpoint);
            } else {
               byte dataPresentx = body.readByte();
               te = new ReadFailureException(cl, received, blockFor, dataPresentx != 0, failureReasonByEndpoint);
            }
            break;
         case 10:
         case 11:
            cl = CBUtil.readConsistencyLevel(body);
            received = body.readInt();
            blockFor = body.readInt();
            if(code == ExceptionCode.WRITE_TIMEOUT) {
               WriteType writeType = (WriteType)Enum.valueOf(WriteType.class, CBUtil.readString(body));
               te = new WriteTimeoutException(writeType, cl, received, blockFor);
            } else {
               byte dataPresent = body.readByte();
               te = new ReadTimeoutException(cl, received, blockFor, dataPresent != 0);
            }
            break;
         case 12:
            String fKeyspace = CBUtil.readString(body);
            String fName = CBUtil.readString(body);
            List<String> argTypes = CBUtil.readStringList(body);
            te = new FunctionExecutionException(new FunctionName(fKeyspace, fName), argTypes, msg);
            break;
         case 13:
            MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
            te = new PreparedQueryNotFoundException(id);
            break;
         case 14:
            te = new SyntaxException(msg);
            break;
         case 15:
            te = new UnauthorizedException(msg);
            break;
         case 16:
            te = new InvalidRequestException(msg);
            break;
         case 17:
            te = new ConfigurationException(msg);
            break;
         case 18:
            String ksName = CBUtil.readString(body);
            String cfName = CBUtil.readString(body);
            if(cfName.isEmpty()) {
               te = new AlreadyExistsException(ksName);
            } else {
               te = new AlreadyExistsException(ksName, cfName);
            }
            break;
         case 19:
            te = new ClientWriteException(msg);
         }

         return new ErrorMessage((TransportException)te, null);
      }

      public void encode(ErrorMessage msg, ByteBuf dest, ProtocolVersion version) {
         TransportException err = ErrorMessage.getBackwardsCompatibleException(msg, version);
         dest.writeInt(err.code().value);
         String errorString = err.getMessage() == null?"":err.getMessage();
         CBUtil.writeString(errorString, dest);
         boolean isWrite;
         switch(null.$SwitchMap$org$apache$cassandra$exceptions$ExceptionCode[err.code().ordinal()]) {
         case 4:
            UnavailableException ue = (UnavailableException)err;
            CBUtil.writeConsistencyLevel(ue.consistency, dest);
            dest.writeInt(ue.required);
            dest.writeInt(ue.alive);
         case 5:
         case 6:
         case 7:
         case 14:
         case 15:
         case 16:
         case 17:
         default:
            break;
         case 8:
         case 9:
            RequestFailureException rfe = (RequestFailureException)err;
            isWrite = err.code() == ExceptionCode.WRITE_FAILURE;
            CBUtil.writeConsistencyLevel(rfe.consistency, dest);
            dest.writeInt(rfe.received);
            dest.writeInt(rfe.blockFor);
            dest.writeInt(rfe.failureReasonByEndpoint.size());
            if(version.isGreaterOrEqualTo(ProtocolVersion.V5)) {
               Iterator var13 = rfe.failureReasonByEndpoint.entrySet().iterator();

               while(var13.hasNext()) {
                  Entry<InetAddress, RequestFailureReason> entry = (Entry)var13.next();
                  CBUtil.writeInetAddr((InetAddress)entry.getKey(), dest);
                  dest.writeShort(((RequestFailureReason)entry.getValue()).codeForNativeProtocol());
               }
            }

            if(isWrite) {
               CBUtil.writeString(((WriteFailureException)rfe).writeType.toString(), dest);
            } else {
               dest.writeByte((byte)(((ReadFailureException)rfe).dataPresent?1:0));
            }
            break;
         case 10:
         case 11:
            RequestTimeoutException rte = (RequestTimeoutException)err;
            isWrite = err.code() == ExceptionCode.WRITE_TIMEOUT;
            CBUtil.writeConsistencyLevel(rte.consistency, dest);
            dest.writeInt(rte.received);
            dest.writeInt(rte.blockFor);
            if(isWrite) {
               CBUtil.writeString(((WriteTimeoutException)rte).writeType.toString(), dest);
            } else {
               dest.writeByte((byte)(((ReadTimeoutException)rte).dataPresent?1:0));
            }
            break;
         case 12:
            FunctionExecutionException fee = (FunctionExecutionException)msg.error;
            CBUtil.writeString(fee.functionName.keyspace, dest);
            CBUtil.writeString(fee.functionName.name, dest);
            CBUtil.writeStringList(fee.argTypes, dest);
            break;
         case 13:
            PreparedQueryNotFoundException pqnfe = (PreparedQueryNotFoundException)err;
            CBUtil.writeBytes(pqnfe.id.bytes, dest);
            break;
         case 18:
            AlreadyExistsException aee = (AlreadyExistsException)err;
            CBUtil.writeString(aee.ksName, dest);
            CBUtil.writeString(aee.cfName, dest);
         }

      }

      public int encodedSize(ErrorMessage msg, ProtocolVersion version) {
         TransportException err = ErrorMessage.getBackwardsCompatibleException(msg, version);
         String errorString = err.getMessage() == null?"":err.getMessage();
         int size = 4 + CBUtil.sizeOfString(errorString);
         boolean isWrite;
         switch(null.$SwitchMap$org$apache$cassandra$exceptions$ExceptionCode[err.code().ordinal()]) {
         case 4:
            UnavailableException ue = (UnavailableException)err;
            size += CBUtil.sizeOfConsistencyLevel(ue.consistency) + 8;
         case 5:
         case 6:
         case 7:
         case 14:
         case 15:
         case 16:
         case 17:
         default:
            break;
         case 8:
         case 9:
            RequestFailureException rfe = (RequestFailureException)err;
            isWrite = err.code() == ExceptionCode.WRITE_FAILURE;
            size += CBUtil.sizeOfConsistencyLevel(rfe.consistency) + 4 + 4 + 4;
            size += isWrite?CBUtil.sizeOfString(((WriteFailureException)rfe).writeType.toString()):1;
            if(version.isGreaterOrEqualTo(ProtocolVersion.V5)) {
               for(Iterator var13 = rfe.failureReasonByEndpoint.entrySet().iterator(); var13.hasNext(); size += 2) {
                  Entry<InetAddress, RequestFailureReason> entry = (Entry)var13.next();
                  size += CBUtil.sizeOfInetAddr((InetAddress)entry.getKey());
               }
            }
            break;
         case 10:
         case 11:
            RequestTimeoutException rte = (RequestTimeoutException)err;
            isWrite = err.code() == ExceptionCode.WRITE_TIMEOUT;
            size += CBUtil.sizeOfConsistencyLevel(rte.consistency) + 8;
            size += isWrite?CBUtil.sizeOfString(((WriteTimeoutException)rte).writeType.toString()):1;
            break;
         case 12:
            FunctionExecutionException fee = (FunctionExecutionException)msg.error;
            size += CBUtil.sizeOfString(fee.functionName.keyspace);
            size += CBUtil.sizeOfString(fee.functionName.name);
            size += CBUtil.sizeOfStringList(fee.argTypes);
            break;
         case 13:
            PreparedQueryNotFoundException pqnfe = (PreparedQueryNotFoundException)err;
            size += CBUtil.sizeOfBytes(pqnfe.id.bytes);
            break;
         case 18:
            AlreadyExistsException aee = (AlreadyExistsException)err;
            size += CBUtil.sizeOfString(aee.ksName);
            size += CBUtil.sizeOfString(aee.cfName);
         }

         return size;
      }
   };
   public final TransportException error;

   private static TransportException getBackwardsCompatibleException(ErrorMessage msg, ProtocolVersion version) {
      if(version.isSmallerThan(ProtocolVersion.V4)) {
         switch(null.$SwitchMap$org$apache$cassandra$exceptions$ExceptionCode[msg.error.code().ordinal()]) {
         case 8:
            WriteFailureException wfe = (WriteFailureException)msg.error;
            return new WriteTimeoutException(wfe.writeType, wfe.consistency, wfe.received, wfe.blockFor);
         case 9:
            ReadFailureException rfe = (ReadFailureException)msg.error;
            return new ReadTimeoutException(rfe.consistency, rfe.received, rfe.blockFor, rfe.dataPresent);
         case 10:
         case 11:
         default:
            break;
         case 12:
            return new InvalidRequestException(msg.toString());
         }
      }

      return msg.error;
   }

   private ErrorMessage(TransportException error) {
      super(Message.Type.ERROR);
      this.error = error;
   }

   private ErrorMessage(TransportException error, int streamId) {
      this(error);
      this.setStreamId(streamId);
   }

   public static ErrorMessage fromException(Throwable e) {
      return fromException(e, (Predicate)null);
   }

   public static ErrorMessage fromException(Throwable e, Predicate<Throwable> unexpectedExceptionHandler) {
      int streamId = 0;
      if(e instanceof CodecException) {
         Throwable cause = e.getCause();
         if(cause != null) {
            if(cause instanceof ErrorMessage.WrappedException) {
               streamId = ((ErrorMessage.WrappedException)cause).streamId;
               e = cause.getCause();
            } else if(cause instanceof TransportException) {
               e = cause;
            }
         }
      } else if(e instanceof ErrorMessage.WrappedException) {
         streamId = ((ErrorMessage.WrappedException)e).streamId;
         e = e.getCause();
      }

      if(e instanceof TransportException || e instanceof RuntimeException && e.getCause() instanceof TransportException) {
         ErrorMessage message = new ErrorMessage(e instanceof TransportException?(TransportException)e:(TransportException)e.getCause(), streamId);
         if(e instanceof ProtocolException) {
            ProtocolVersion forcedProtocolVersion = ((ProtocolException)e).getForcedProtocolVersion();
            if(forcedProtocolVersion != null) {
               message.forcedProtocolVersion = forcedProtocolVersion;
            }
         }

         return message;
      } else {
         if(unexpectedExceptionHandler == null || !unexpectedExceptionHandler.apply(e)) {
            logger.error("Unexpected exception during request", e);
         }

         return new ErrorMessage(new ServerError(e), streamId);
      }
   }

   public String toString() {
      return "ERROR " + this.error.code() + ": " + this.error.getMessage();
   }

   public static RuntimeException wrap(Throwable t, int streamId) {
      return new ErrorMessage.WrappedException(t, streamId);
   }

   public static class WrappedException extends RuntimeException {
      private final int streamId;

      public WrappedException(Throwable cause, int streamId) {
         super(cause);
         this.streamId = streamId;
      }

      @VisibleForTesting
      public int getStreamId() {
         return this.streamId;
      }
   }
}
