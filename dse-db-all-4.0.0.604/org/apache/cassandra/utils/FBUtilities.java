package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FBUtilities {
   private static final Logger logger = LoggerFactory.getLogger(FBUtilities.class);
   private static final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
   public static final BigInteger TWO = new BigInteger("2");
   private static final String DEFAULT_TRIGGER_DIR = "triggers";
   public static final String OPERATING_SYSTEM = System.getProperty("os.name").toLowerCase();
   public static final boolean isWindows;
   public static final boolean isLinux;
   public static final boolean isMacOSX;
   private static volatile InetAddress localInetAddress;
   private static volatile InetAddress broadcastInetAddress;
   private static volatile InetAddress broadcastRpcAddress;
   public static final int MAX_UNSIGNED_SHORT = 65535;

   public FBUtilities() {
   }

   public static int getAvailableProcessors() {
      String availableProcessors = System.getProperty("cassandra.available_processors");
      return !Strings.isNullOrEmpty(availableProcessors)?Integer.parseInt(availableProcessors):Runtime.getRuntime().availableProcessors();
   }

   public static InetAddress getLocalAddress() {
      if(localInetAddress == null) {
         try {
            localInetAddress = DatabaseDescriptor.getListenAddress() == null?InetAddress.getLocalHost():DatabaseDescriptor.getListenAddress();
         } catch (UnknownHostException var1) {
            throw new RuntimeException(var1);
         }
      }

      return localInetAddress;
   }

   public static InetAddress getBroadcastAddress() {
      if(broadcastInetAddress == null) {
         broadcastInetAddress = DatabaseDescriptor.getBroadcastAddress() == null?getLocalAddress():DatabaseDescriptor.getBroadcastAddress();
      }

      return broadcastInetAddress;
   }

   public static InetAddress getNativeTransportBroadcastAddress() {
      if(broadcastRpcAddress == null) {
         broadcastRpcAddress = DatabaseDescriptor.getBroadcastNativeTransportAddress() == null?DatabaseDescriptor.getNativeTransportAddress():DatabaseDescriptor.getBroadcastNativeTransportAddress();
      }

      return broadcastRpcAddress;
   }

   public static Collection<InetAddress> getAllLocalAddresses() {
      HashSet localAddresses = new HashSet();

      try {
         Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
         if(nets != null) {
            while(nets.hasMoreElements()) {
               localAddresses.addAll(Collections.list(((NetworkInterface)nets.nextElement()).getInetAddresses()));
            }
         }

         return localAddresses;
      } catch (SocketException var2) {
         throw new AssertionError(var2);
      }
   }

   public static String getNetworkInterface(InetAddress localAddress) {
      try {
         Iterator var1 = Collections.list(NetworkInterface.getNetworkInterfaces()).iterator();

         while(true) {
            NetworkInterface ifc;
            do {
               if(!var1.hasNext()) {
                  return null;
               }

               ifc = (NetworkInterface)var1.next();
            } while(!ifc.isUp());

            Iterator var3 = Collections.list(ifc.getInetAddresses()).iterator();

            while(var3.hasNext()) {
               InetAddress addr = (InetAddress)var3.next();
               if(addr.equals(localAddress)) {
                  return ifc.getDisplayName();
               }
            }
         }
      } catch (SocketException var5) {
         return null;
      }
   }

   public static Pair<BigInteger, Boolean> midpoint(BigInteger left, BigInteger right, int sigbits) {
      BigInteger midpoint;
      boolean remainder;
      BigInteger sum;
      if(left.compareTo(right) < 0) {
         sum = left.add(right);
         remainder = sum.testBit(0);
         midpoint = sum.shiftRight(1);
      } else {
         sum = TWO.pow(sigbits);
         BigInteger distance = sum.add(right).subtract(left);
         remainder = distance.testBit(0);
         midpoint = distance.shiftRight(1).add(left).mod(sum);
      }

      return Pair.create(midpoint, Boolean.valueOf(remainder));
   }

   public static int compareUnsigned(byte[] bytes1, byte[] bytes2, int offset1, int offset2, int len1, int len2) {
      return FastByteOperations.compareUnsigned(bytes1, offset1, len1, bytes2, offset2, len2);
   }

   public static int compareUnsigned(byte[] bytes1, byte[] bytes2) {
      return compareUnsigned(bytes1, bytes2, 0, 0, bytes1.length, bytes2.length);
   }

   public static byte[] xor(byte[] left, byte[] right) {
      if(left != null && right != null) {
         byte[] out;
         if(left.length > right.length) {
            out = left;
            left = right;
            right = out;
         }

         out = Arrays.copyOf(right, right.length);

         for(int i = 0; i < left.length; ++i) {
            out[i] = (byte)(left[i] & 255 ^ right[i] & 255);
         }

         return out;
      } else {
         return null;
      }
   }

   public static void sortSampledKeys(List<DecoratedKey> keys, Range<Token> range) {
      if(((Token)range.left).compareTo(range.right) >= 0) {
         final Token right = (Token)range.right;
         Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>() {
            public int compare(DecoratedKey o1, DecoratedKey o2) {
               return (right.compareTo(o1.getToken()) >= 0 || right.compareTo(o2.getToken()) >= 0) && (right.compareTo(o1.getToken()) <= 0 || right.compareTo(o2.getToken()) <= 0)?o2.compareTo((PartitionPosition)o1):o1.compareTo((PartitionPosition)o2);
            }
         };
         Collections.sort(keys, comparator);
      } else {
         Collections.sort(keys);
      }

   }

   public static String resourceToFile(String filename) throws ConfigurationException {
      ClassLoader loader = FBUtilities.class.getClassLoader();
      URL scpurl = loader.getResource(filename);
      if(scpurl == null) {
         throw new ConfigurationException("unable to locate " + filename);
      } else {
         return (new File(scpurl.getFile())).getAbsolutePath();
      }
   }

   public static File cassandraTriggerDir() {
      File triggerDir = null;
      if(System.getProperty("cassandra.triggers_dir") != null) {
         triggerDir = new File(System.getProperty("cassandra.triggers_dir"));
      } else {
         URL confDir = FBUtilities.class.getClassLoader().getResource("triggers");
         if(confDir != null) {
            triggerDir = new File(confDir.getFile());
         }
      }

      if(triggerDir != null && triggerDir.exists()) {
         return triggerDir;
      } else {
         logger.warn("Trigger directory doesn't exist, please create it and try again.");
         return null;
      }
   }

   public static String getReleaseVersionString() {
      return FBUtilities.Versions.releaseVersionString;
   }

   public static String getDSEVersionString() {
      return FBUtilities.Versions.dseVersionString;
   }

   public static long timestampMicros() {
      return System.currentTimeMillis() * 1000L;
   }

   public static int nowInSeconds() {
      return (int)(System.currentTimeMillis() / 1000L);
   }

   public static <T> List<T> waitOnFutures(Iterable<? extends Future<? extends T>> futures) {
      return waitOnFutures(futures, -1L);
   }

   public static <T> List<T> waitOnFutures(Iterable<? extends Future<? extends T>> futures, long ms) {
      List<T> results = new ArrayList();
      Throwable fail = null;
      Iterator var5 = futures.iterator();

      while(var5.hasNext()) {
         Future f = (Future)var5.next();

         try {
            if(ms <= 0L) {
               results.add(f.get());
            } else {
               results.add(f.get(ms, TimeUnit.MILLISECONDS));
            }
         } catch (Throwable var8) {
            fail = Throwables.merge(fail, var8);
         }
      }

      Throwables.maybeFail(fail);
      return results;
   }

   public static <T> T waitOnFuture(Future<T> future) {
      try {
         return future.get();
      } catch (ExecutionException var2) {
         throw new RuntimeException(var2);
      } catch (InterruptedException var3) {
         throw new AssertionError(var3);
      }
   }

   public static <T> Future<? extends T> waitOnFirstFuture(Iterable<? extends Future<? extends T>> futures) {
      return waitOnFirstFuture(futures, 100L);
   }

   public static <T> Future<? extends T> waitOnFirstFuture(Iterable<? extends Future<? extends T>> futures, long delay) {
      label23:
      while(true) {
         Iterator var3 = futures.iterator();

         Future f;
         do {
            if(!var3.hasNext()) {
               Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
               continue label23;
            }

            f = (Future)var3.next();
         } while(!f.isDone());

         try {
            f.get();
            return f;
         } catch (InterruptedException var6) {
            throw new AssertionError(var6);
         } catch (ExecutionException var7) {
            throw new RuntimeException(var7);
         }
      }
   }

   public static IPartitioner newPartitioner(Descriptor desc) throws IOException {
      EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.HEADER);
      Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
      ValidationMetadata validationMetadata = (ValidationMetadata)sstableMetadata.get(MetadataType.VALIDATION);
      SerializationHeader.Component header = (SerializationHeader.Component)sstableMetadata.get(MetadataType.HEADER);
      return newPartitioner(validationMetadata.partitioner, Optional.of(header.getKeyType()));
   }

   public static IPartitioner newPartitioner(String partitionerClassName) throws ConfigurationException {
      return newPartitioner(partitionerClassName, Optional.empty());
   }

   @VisibleForTesting
   static IPartitioner newPartitioner(String partitionerClassName, Optional<AbstractType<?>> comparator) throws ConfigurationException {
      if(!partitionerClassName.contains(".")) {
         partitionerClassName = "org.apache.cassandra.dht." + partitionerClassName;
      }

      if(partitionerClassName.equals("org.apache.cassandra.dht.LocalPartitioner")) {
         assert comparator.isPresent() : "Expected a comparator for local partitioner";

         return new LocalPartitioner((AbstractType)comparator.get());
      } else {
         return (IPartitioner)instanceOrConstruct(partitionerClassName, "partitioner");
      }
   }

   public static IAuthorizer newAuthorizer(String className) throws ConfigurationException {
      if(!className.contains(".")) {
         className = "org.apache.cassandra.auth." + className;
      }

      return (IAuthorizer)construct(className, "authorizer");
   }

   public static IAuthenticator newAuthenticator(String className) throws ConfigurationException {
      if(!className.contains(".")) {
         className = "org.apache.cassandra.auth." + className;
      }

      return (IAuthenticator)construct(className, "authenticator");
   }

   public static IRoleManager newRoleManager(String className) throws ConfigurationException {
      if(!className.contains(".")) {
         className = "org.apache.cassandra.auth." + className;
      }

      return (IRoleManager)construct(className, "role manager");
   }

   public static <T> Class<T> classForName(String classname, String readable) throws ConfigurationException {
      try {
         return Class.forName(classname);
      } catch (NoClassDefFoundError | ClassNotFoundException var3) {
         throw new ConfigurationException(String.format("Unable to find %s class '%s'", new Object[]{readable, classname}), var3);
      }
   }

   public static <T> T instanceOrConstruct(String classname, String readable) throws ConfigurationException {
      Class cls = classForName(classname, readable);

      try {
         Field instance = cls.getField("instance");
         return cls.cast(instance.get((Object)null));
      } catch (SecurityException | IllegalArgumentException | IllegalAccessException | NoSuchFieldException var4) {
         return construct(cls, classname, readable);
      }
   }

   public static <T> T construct(String classname, String readable) throws ConfigurationException {
      Class<T> cls = classForName(classname, readable);
      return construct(cls, classname, readable);
   }

   private static <T> T construct(Class<T> cls, String classname, String readable) throws ConfigurationException {
      try {
         return cls.newInstance();
      } catch (IllegalAccessException var4) {
         throw new ConfigurationException(String.format("Default constructor for %s class '%s' is inaccessible.", new Object[]{readable, classname}));
      } catch (InstantiationException var5) {
         throw new ConfigurationException(String.format("Cannot use abstract class '%s' as %s.", new Object[]{classname, readable}));
      } catch (Exception var6) {
         if(var6.getCause() instanceof ConfigurationException) {
            throw (ConfigurationException)var6.getCause();
         } else {
            throw new ConfigurationException(String.format("Error instantiating %s class '%s'.", new Object[]{readable, classname}), var6);
         }
      }
   }

   public static <T> NavigableSet<T> singleton(T column, Comparator<? super T> comparator) {
      NavigableSet<T> s = new TreeSet(comparator);
      s.add(column);
      return s;
   }

   public static <T> NavigableSet<T> emptySortedSet(Comparator<? super T> comparator) {
      return new TreeSet(comparator);
   }

   @Nonnull
   public static String toString(@Nullable Map<?, ?> map) {
      if(map == null) {
         return "";
      } else {
         MapJoiner joiner = Joiner.on(", ").withKeyValueSeparator(":");
         return joiner.join(map);
      }
   }

   public static Field getProtectedField(String className, String fieldName) {
      try {
         return getProtectedField(Class.forName(className), fieldName);
      } catch (Exception var3) {
         throw new IllegalStateException(var3);
      }
   }

   public static Field getProtectedField(Class<?> klass, String fieldName) {
      try {
         Field field = klass.getDeclaredField(fieldName);
         field.setAccessible(true);
         return field;
      } catch (Exception var3) {
         throw new IllegalStateException(var3);
      }
   }

   public static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator) {
      return new FBUtilities.WrappedCloseableIterator(iterator);
   }

   public static Map<String, String> fromJsonMap(String json) {
      try {
         return (Map)jsonMapper.readValue(json, Map.class);
      } catch (IOException var2) {
         throw new RuntimeException(var2);
      }
   }

   public static List<String> fromJsonList(String json) {
      try {
         return (List)jsonMapper.readValue(json, List.class);
      } catch (IOException var2) {
         throw new RuntimeException(var2);
      }
   }

   public static String json(Object object) {
      try {
         return jsonMapper.writeValueAsString(object);
      } catch (IOException var2) {
         throw new RuntimeException(var2);
      }
   }

   public static String prettyPrintMemory(long size) {
      return prettyPrintMemory(size, false);
   }

   public static String prettyPrintMemory(long size, boolean includeSpace) {
      return size >= 1073741824L?String.format("%.3f%sGiB", new Object[]{Double.valueOf((double)size / 1.073741824E9D), includeSpace?" ":""}):(size >= 1048576L?String.format("%.3f%sMiB", new Object[]{Double.valueOf((double)size / 1048576.0D), includeSpace?" ":""}):String.format("%.3f%sKiB", new Object[]{Double.valueOf((double)size / 1024.0D), includeSpace?" ":""}));
   }

   public static String prettyPrintMemoryPerSecond(long rate) {
      return rate >= 1073741824L?String.format("%.3fGiB/s", new Object[]{Double.valueOf((double)rate / 1.073741824E9D)}):(rate >= 1048576L?String.format("%.3fMiB/s", new Object[]{Double.valueOf((double)rate / 1048576.0D)}):String.format("%.3fKiB/s", new Object[]{Double.valueOf((double)rate / 1024.0D)}));
   }

   public static String prettyPrintMemoryPerSecond(long bytes, long timeInNano) {
      if(timeInNano == 0L) {
         return "NaN  KiB/s";
      } else {
         long rate = (long)((double)bytes / (double)timeInNano * 1000.0D * 1000.0D * 1000.0D);
         return prettyPrintMemoryPerSecond(rate);
      }
   }

   public static void exec(ProcessBuilder pb) throws IOException {
      Process p = pb.start();

      try {
         int errCode = p.waitFor();
         if(errCode != 0) {
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            Throwable var4 = null;

            try {
               BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
               Throwable var6 = null;

               try {
                  String lineSep = System.getProperty("line.separator");
                  StringBuilder sb = new StringBuilder();

                  String str;
                  while((str = in.readLine()) != null) {
                     sb.append(str).append(lineSep);
                  }

                  while((str = err.readLine()) != null) {
                     sb.append(str).append(lineSep);
                  }

                  throw new IOException("Exception while executing the command: " + StringUtils.join(pb.command(), " ") + ", command error Code: " + errCode + ", command output: " + sb.toString());
               } catch (Throwable var30) {
                  var6 = var30;
                  throw var30;
               } finally {
                  if(err != null) {
                     if(var6 != null) {
                        try {
                           err.close();
                        } catch (Throwable var29) {
                           var6.addSuppressed(var29);
                        }
                     } else {
                        err.close();
                     }
                  }

               }
            } catch (Throwable var32) {
               var4 = var32;
               throw var32;
            } finally {
               if(in != null) {
                  if(var4 != null) {
                     try {
                        in.close();
                     } catch (Throwable var28) {
                        var4.addSuppressed(var28);
                     }
                  } else {
                     in.close();
                  }
               }

            }
         }
      } catch (InterruptedException var34) {
         throw new AssertionError(var34);
      }
   }

   public static void updateChecksumInt(Checksum checksum, int v) {
      checksum.update(v >>> 24 & 255);
      checksum.update(v >>> 16 & 255);
      checksum.update(v >>> 8 & 255);
      checksum.update(v >>> 0 & 255);
   }

   public static void updateChecksum(CRC32 checksum, ByteBuffer buffer, int offset, int length) {
      int position = buffer.position();
      int limit = buffer.limit();
      buffer.position(offset).limit(offset + length);
      checksum.update(buffer);
      buffer.position(position).limit(limit);
   }

   public static void updateChecksum(CRC32 checksum, ByteBuffer buffer) {
      int position = buffer.position();
      checksum.update(buffer);
      buffer.position(position);
   }

   public static long abs(long index) {
      long negbit = index >> 63;
      return (index ^ negbit) - negbit;
   }

   public static long copy(InputStream from, OutputStream to, long limit) throws IOException {
      byte[] buffer = new byte[64];
      long copied = 0L;
      int toCopy = buffer.length;

      do {
         if(limit < (long)buffer.length + copied) {
            toCopy = (int)(limit - copied);
         }

         int sofar = from.read(buffer, 0, toCopy);
         if(sofar == -1) {
            break;
         }

         to.write(buffer, 0, sofar);
         copied += (long)sofar;
      } while(limit != copied);

      return copied;
   }

   public static File getToolsOutputDirectory() {
      File historyDir = new File(System.getProperty("user.home"), ".cassandra");
      FileUtils.createDirectory(historyDir);
      return historyDir;
   }

   public static void closeAll(Collection<? extends AutoCloseable> l) throws Exception {
      Exception toThrow = null;
      Iterator var2 = l.iterator();

      while(var2.hasNext()) {
         AutoCloseable c = (AutoCloseable)var2.next();

         try {
            c.close();
         } catch (Exception var5) {
            if(toThrow == null) {
               toThrow = var5;
            } else {
               toThrow.addSuppressed(var5);
            }
         }
      }

      if(toThrow != null) {
         throw toThrow;
      }
   }

   public static byte[] toWriteUTFBytes(String s) {
      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos);
         dos.writeUTF(s);
         dos.flush();
         return baos.toByteArray();
      } catch (IOException var3) {
         throw new RuntimeException(var3);
      }
   }

   public static void sleepQuietly(long millis) {
      try {
         Thread.sleep(millis);
      } catch (InterruptedException var3) {
         throw new RuntimeException(var3);
      }
   }

   public static long align(long val, int boundary) {
      return val + (long)boundary - 1L & (long)(~(boundary - 1));
   }

   @VisibleForTesting
   protected static void reset() {
      localInetAddress = null;
      broadcastInetAddress = null;
      broadcastRpcAddress = null;
   }

   public static long add(long x, long y) {
      long r = x + y;
      return ((x ^ r) & (y ^ r)) < 0L?9223372036854775807L:r;
   }

   public static int add(int x, int y) {
      int r = x + y;
      return ((x ^ r) & (y ^ r)) < 0?2147483647:r;
   }

   public static String execBlocking(String[] cmd, int timeout, TimeUnit timeUnit) {
      try {
         Process proc = Runtime.getRuntime().exec(cmd);
         if(!proc.waitFor((long)timeout, timeUnit)) {
            String out = toString(proc.getInputStream());
            proc.destroy();
            throw new RuntimeException(String.format("<%s> did not terminate within %d %s. Partial output:\n%s", new Object[]{cmd, Integer.valueOf(timeout), timeUnit.name().toLowerCase(), out}));
         } else if(proc.exitValue() != 0) {
            throw new RuntimeException(String.format("<%s> failed with error code %d:\n%s", new Object[]{cmd, Integer.valueOf(proc.exitValue()), toString(proc.getErrorStream())}));
         } else {
            return toString(proc.getInputStream());
         }
      } catch (Exception var5) {
         throw new RuntimeException(var5);
      }
   }

   private static String toString(InputStream in) throws IOException {
      byte[] buf = new byte[in.available()];

      int rd;
      for(int p = 0; p < buf.length; p += rd) {
         rd = in.read(buf, p, buf.length - p);
         if(rd < 0) {
            break;
         }
      }

      return new String(buf, StandardCharsets.UTF_8);
   }

   static {
      isWindows = OPERATING_SYSTEM.contains("windows");
      isLinux = OPERATING_SYSTEM.contains("linux");
      isMacOSX = OPERATING_SYSTEM.contains("mac os x");
   }

   public static class CpuInfo {
      private final List<FBUtilities.CpuInfo.PhysicalProcessor> processors = new ArrayList();

      @VisibleForTesting
      protected CpuInfo() {
      }

      public static String niceCpuIdList(List<Integer> cpuIds) {
         StringBuilder sb = new StringBuilder();
         Integer rangeStart = null;
         Integer previous = null;

         Integer id;
         for(Iterator i = cpuIds.iterator(); i.hasNext(); previous = id) {
            id = (Integer)i.next();
            if(rangeStart == null) {
               rangeStart = id;
            } else if(previous.intValue() + 1 != id.intValue()) {
               if(sb.length() > 0) {
                  sb.append(',');
               }

               if(previous.equals(rangeStart)) {
                  sb.append(rangeStart);
               } else {
                  sb.append(rangeStart).append('-').append(previous);
               }

               rangeStart = id;
            }
         }

         if(rangeStart != null) {
            if(sb.length() > 0) {
               sb.append(',');
            }

            if(previous.equals(rangeStart)) {
               sb.append(rangeStart);
            } else {
               sb.append(rangeStart).append('-').append(previous);
            }
         }

         return sb.toString();
      }

      public List<FBUtilities.CpuInfo.PhysicalProcessor> getProcessors() {
         return this.processors;
      }

      public static FBUtilities.CpuInfo load() {
         File fCpuInfo = new File("/proc/cpuinfo");
         if(!fCpuInfo.exists()) {
            throw new IOError(new FileNotFoundException(fCpuInfo.getAbsolutePath()));
         } else {
            List<String> cpuinfoLines = FileUtils.readLines(fCpuInfo);
            return loadFrom(cpuinfoLines);
         }
      }

      public static FBUtilities.CpuInfo loadFrom(List<String> lines) {
         FBUtilities.CpuInfo cpuInfo = new FBUtilities.CpuInfo();
         Pattern linePattern = Pattern.compile("^([A-Za-z _-]+[A-Za-z_-])\\s*[:] (.*)$");
         Map<String, String> info = new HashMap();
         Iterator var4 = lines.iterator();

         while(var4.hasNext()) {
            String cpuinfoLine = (String)var4.next();
            if(cpuinfoLine.isEmpty()) {
               cpuInfo.addCpu(info);
               info.clear();
            } else {
               Matcher matcher = linePattern.matcher(cpuinfoLine);
               if(matcher.matches()) {
                  info.put(matcher.group(1), matcher.group(2));
               }
            }
         }

         cpuInfo.addCpu(info);
         return cpuInfo;
      }

      private void addCpu(Map<String, String> info) {
         if(!info.isEmpty()) {
            try {
               int physicalId = Integer.parseInt((String)info.get("physical id"));
               int cores = Integer.parseInt((String)info.get("cpu cores"));
               int cpuId = Integer.parseInt((String)info.get("processor"));
               int coreId = Integer.parseInt((String)info.get("core id"));
               String modelName = (String)info.get("model name");
               String mhz = (String)info.get("cpu MHz");
               String cacheSize = (String)info.get("cache size");
               String vendorId = (String)info.get("vendor_id");
               Set<String> flags = new HashSet(Arrays.asList(((String)info.get("flags")).split(" ")));
               FBUtilities.CpuInfo.PhysicalProcessor processor = this.getProcessor(physicalId);
               if(processor == null) {
                  processor = new FBUtilities.CpuInfo.PhysicalProcessor(modelName, physicalId, mhz, cacheSize, vendorId, flags, cores);
                  this.processors.add(processor);
               }

               processor.addCpu(coreId, cpuId);
            } catch (Exception var12) {
               ;
            }

         }
      }

      private FBUtilities.CpuInfo.PhysicalProcessor getProcessor(int physicalId) {
         return (FBUtilities.CpuInfo.PhysicalProcessor)this.processors.stream().filter((p) -> {
            return p.physicalId == physicalId;
         }).findFirst().orElse((Object)null);
      }

      public String fetchCpuScalingGovernor(int cpuId) {
         File cpuDir = new File(String.format("/sys/devices/system/cpu/cpu%d/cpufreq", new Object[]{Integer.valueOf(cpuId)}));
         if(!cpuDir.canRead()) {
            return "no_cpufreq";
         } else {
            File file = new File(cpuDir, "scaling_governor");
            if(!file.canRead()) {
               return "no_scaling_governor";
            } else {
               String governor = FileUtils.readLine(file);
               return governor != null?governor:"unknown";
            }
         }
      }

      public int cpuCount() {
         return this.processors.stream().mapToInt(FBUtilities.CpuInfo.PhysicalProcessor::cpuCount).sum();
      }

      public static class PhysicalProcessor {
         private final int physicalId;
         private final String name;
         private final String mhz;
         private final String cacheSize;
         private final String vendorId;
         private final Set<String> flags;
         private final int cores;
         private final BitSet cpuIds = new BitSet();
         private final BitSet coreIds = new BitSet();

         PhysicalProcessor(String name, int physicalId, String mhz, String cacheSize, String vendorId, Set<String> flags, int cores) {
            this.name = name;
            this.physicalId = physicalId;
            this.mhz = mhz;
            this.cacheSize = cacheSize;
            this.vendorId = vendorId;
            this.flags = flags;
            this.cores = cores;
         }

         public String getName() {
            return this.name;
         }

         public int getPhysicalId() {
            return this.physicalId;
         }

         public String getMhz() {
            return this.mhz;
         }

         public int getCores() {
            return this.cores;
         }

         public String getCacheSize() {
            return this.cacheSize;
         }

         public String getVendorId() {
            return this.vendorId;
         }

         public Set<String> getFlags() {
            return this.flags;
         }

         public boolean hasFlag(String flag) {
            return this.flags.contains(flag);
         }

         public int getThreadsPerCore() {
            return this.cpuIds.cardinality() / this.coreIds.cardinality();
         }

         public IntStream cpuIds() {
            return this.cpuIds.stream();
         }

         void addCpu(int coreId, int cpuId) {
            this.cpuIds.set(cpuId);
            this.coreIds.set(coreId);
         }

         public int cpuCount() {
            return this.cpuIds.cardinality();
         }
      }
   }

   public static final class Debug {
      private static final Map<Object, FBUtilities.Debug.ThreadInfo> stacks = new ConcurrentHashMap();

      public Debug() {
      }

      public static String getStackTrace() {
         return getStackTrace(new FBUtilities.Debug.ThreadInfo());
      }

      public static String getStackTrace(Thread thread) {
         return getStackTrace(new FBUtilities.Debug.ThreadInfo(thread));
      }

      public static String getStackTrace(FBUtilities.Debug.ThreadInfo threadInfo) {
         StringBuilder sb = new StringBuilder();
         sb.append("Thread ").append(threadInfo.name).append(" (").append(threadInfo.isDaemon?"daemon":"non-daemon").append(")").append("\n");
         StackTraceElement[] var2 = threadInfo.stack;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            StackTraceElement element = var2[var4];
            sb.append(element);
            sb.append("\n");
         }

         return sb.toString();
      }

      public static void addStackTrace(Object object) {
         stacks.put(object, new FBUtilities.Debug.ThreadInfo());
      }

      public static void logStackTrace(String message, Object object) {
         FBUtilities.logger.info("{}\n{}\n****\n{}", new Object[]{message, getStackTrace((FBUtilities.Debug.ThreadInfo)stacks.get(object)), getStackTrace()});
      }

      public static final class ThreadInfo {
         private final String name;
         private final boolean isDaemon;
         private final StackTraceElement[] stack;

         public ThreadInfo() {
            this(Thread.currentThread());
         }

         public ThreadInfo(Thread thread) {
            this.name = thread.getName();
            this.isDaemon = thread.isDaemon();
            this.stack = thread.getStackTrace();
         }
      }
   }

   private static final class WrappedCloseableIterator<T> extends AbstractIterator<T> implements CloseableIterator<T> {
      private final Iterator<T> source;

      public WrappedCloseableIterator(Iterator<T> source) {
         this.source = source;
      }

      protected T computeNext() {
         return !this.source.hasNext()?this.endOfData():this.source.next();
      }

      public void close() {
      }
   }

   private static final class Versions {
      private static final String releaseVersionString;
      private static final String dseVersionString;

      private Versions() {
      }

      static {
         try {
            InputStream in = FBUtilities.class.getClassLoader().getResourceAsStream("org/apache/cassandra/config/version.properties");
            Throwable var1 = null;

            try {
               Properties props = new Properties();
               props.load(in);
               releaseVersionString = props.getProperty("CassandraVersion");
               dseVersionString = props.getProperty("DSEVersion");
            } catch (Throwable var11) {
               var1 = var11;
               throw var11;
            } finally {
               if(in != null) {
                  if(var1 != null) {
                     try {
                        in.close();
                     } catch (Throwable var10) {
                        var1.addSuppressed(var10);
                     }
                  } else {
                     in.close();
                  }
               }

            }

         } catch (Exception var13) {
            JVMStabilityInspector.inspectThrowable(var13);
            FBUtilities.logger.warn("Unable to load version.properties", var13);
            throw new RuntimeException(var13);
         }
      }
   }
}
