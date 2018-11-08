package org.apache.cassandra.io.util;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInput;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

public final class FileUtils {
   public static final Charset CHARSET;
   private static final Logger logger;
   public static final long ONE_KB = 1024L;
   public static final long ONE_MB = 1048576L;
   public static final long ONE_GB = 1073741824L;
   public static final long ONE_TB = 1099511627776L;
   private static final DecimalFormat df;
   public static final boolean isCleanerAvailable;
   static String[] CPUID_COMMANDLINE;

   public static void createHardLink(String from, String to) {
      createHardLink(new File(from), new File(to));
   }

   public static void createHardLink(File from, File to) {
      if(to.exists()) {
         throw new RuntimeException("Tried to create duplicate hard link to " + to);
      } else if(!from.exists()) {
         throw new RuntimeException("Tried to hard link to file that does not exist " + from);
      } else {
         try {
            Files.createLink(to.toPath(), from.toPath());
         } catch (IOException var3) {
            throw new FSWriteError(var3, to);
         }
      }
   }

   public static File createTempFile(String prefix, String suffix, File directory) {
      try {
         return File.createTempFile(prefix, suffix, directory);
      } catch (IOException var4) {
         throw new FSWriteError(var4, directory);
      }
   }

   public static File createTempFile(String prefix, String suffix) {
      return createTempFile(prefix, suffix, new File(System.getProperty("java.io.tmpdir")));
   }

   public static File createDeletableTempFile(String prefix, String suffix) {
      File f = createTempFile(prefix, suffix, new File(System.getProperty("java.io.tmpdir")));
      f.deleteOnExit();
      return f;
   }

   public static Throwable deleteWithConfirm(String filePath, boolean expect, Throwable accumulate) {
      return deleteWithConfirm(new File(filePath), expect, accumulate);
   }

   public static Throwable deleteWithConfirm(File file, boolean expect, Throwable accumulate) {
      boolean exists = file.exists();

      assert exists || !expect : "attempted to delete non-existing file " + file.getName();

      try {
         if(exists) {
            Files.delete(file.toPath());
         }
      } catch (Throwable var7) {
         Throwable t = var7;

         try {
            throw new FSWriteError(t, file);
         } catch (Throwable var6) {
            accumulate = Throwables.merge(accumulate, var6);
         }
      }

      return accumulate;
   }

   public static void deleteWithConfirm(String file) {
      deleteWithConfirm(new File(file));
   }

   public static void deleteWithConfirm(File file) {
      Throwables.maybeFail(deleteWithConfirm((File)file, true, (Throwable)null));
   }

   public static void renameWithOutConfirm(String from, String to) {
      try {
         atomicMoveWithFallback((new File(from)).toPath(), (new File(to)).toPath());
      } catch (IOException var3) {
         if(logger.isTraceEnabled()) {
            logger.trace("Could not move file " + from + " to " + to, var3);
         }
      }

   }

   public static void renameWithConfirm(String from, String to) {
      renameWithConfirm(new File(from), new File(to));
   }

   public static void renameWithConfirm(File from, File to) {
      assert from.exists();

      if(logger.isTraceEnabled()) {
         logger.trace("Renaming {} to {}", from.getPath(), to.getPath());
      }

      try {
         atomicMoveWithFallback(from.toPath(), to.toPath());
      } catch (IOException var3) {
         throw new RuntimeException(String.format("Failed to rename %s to %s", new Object[]{from.getPath(), to.getPath()}), var3);
      }
   }

   private static void atomicMoveWithFallback(Path from, Path to) throws IOException {
      try {
         Files.move(from, to, new CopyOption[]{StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE});
      } catch (AtomicMoveNotSupportedException var3) {
         logger.trace("Could not do an atomic move", var3);
         Files.move(from, to, new CopyOption[]{StandardCopyOption.REPLACE_EXISTING});
      }

   }

   public static void truncate(String path, long size) {
      try {
         FileChannel channel = FileChannel.open(Paths.get(path, new String[0]), new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE});
         Throwable var4 = null;

         try {
            channel.truncate(size);
         } catch (Throwable var14) {
            var4 = var14;
            throw var14;
         } finally {
            if(channel != null) {
               if(var4 != null) {
                  try {
                     channel.close();
                  } catch (Throwable var13) {
                     var4.addSuppressed(var13);
                  }
               } else {
                  channel.close();
               }
            }

         }

      } catch (IOException var16) {
         throw new RuntimeException(var16);
      }
   }

   public static void closeQuietly(Closeable c) {
      try {
         if(c != null) {
            c.close();
         }
      } catch (Exception var2) {
         logger.warn("Failed closing {}", c, var2);
      }

   }

   public static void closeQuietly(AutoCloseable c) {
      try {
         if(c != null) {
            c.close();
         }
      } catch (Exception var2) {
         JVMStabilityInspector.inspectThrowable(var2);
         logger.warn("Failed closing {}", c, var2);
      }

   }

   public static void close(Closeable... cs) throws IOException {
      close((Iterable)Arrays.asList(cs));
   }

   public static void close(Iterable<? extends Closeable> cs) throws IOException {
      IOException e = null;
      Iterator var2 = cs.iterator();

      while(var2.hasNext()) {
         Closeable c = (Closeable)var2.next();

         try {
            if(c != null) {
               c.close();
            }
         } catch (IOException var5) {
            e = var5;
            logger.warn("Failed closing stream {}", c, var5);
         }
      }

      if(e != null) {
         throw e;
      }
   }

   public static void closeQuietly(Iterable<? extends AutoCloseable> cs) {
      Iterator var1 = cs.iterator();

      while(var1.hasNext()) {
         AutoCloseable c = (AutoCloseable)var1.next();

         try {
            if(c != null) {
               c.close();
            }
         } catch (Exception var4) {
            logger.warn("Failed closing {}", c, var4);
         }
      }

   }

   public static String getCanonicalPath(String filename) {
      try {
         return (new File(filename)).getCanonicalPath();
      } catch (IOException var2) {
         throw new FSReadError(var2, filename);
      }
   }

   public static String getCanonicalPath(File file) {
      try {
         return file.getCanonicalPath();
      } catch (IOException var2) {
         throw new FSReadError(var2, file);
      }
   }

   public static boolean isContained(File folder, File file) {
      String folderPath = getCanonicalPath(folder);
      String filePath = getCanonicalPath(file);
      return filePath.startsWith(folderPath);
   }

   public static String getRelativePath(String basePath, String path) {
      try {
         return Paths.get(basePath, new String[0]).relativize(Paths.get(path, new String[0])).toString();
      } catch (Exception var4) {
         String absDataPath = getCanonicalPath(basePath);
         return Paths.get(absDataPath, new String[0]).relativize(Paths.get(path, new String[0])).toString();
      }
   }

   public static void clean(ByteBuffer buffer) {
      clean(buffer, false);
   }

   public static void clean(ByteBuffer buffer, boolean cleanAttachment) {
      if(buffer != null) {
         if(isCleanerAvailable && buffer.isDirect()) {
            DirectBuffer db = (DirectBuffer)buffer;
            if(db.cleaner() != null) {
               db.cleaner().clean();
            } else if(cleanAttachment && buffer.isDirect()) {
               Object attach = UnsafeByteBufferAccess.getAttachment(buffer);
               if(attach != null && attach instanceof ByteBuffer && attach != buffer) {
                  clean((ByteBuffer)attach);
               }
            }
         }

      }
   }

   public static void createDirectory(String directory) {
      createDirectory(new File(directory));
   }

   public static void createDirectory(File directory) {
      if(!directory.exists() && !directory.mkdirs()) {
         throw new FSWriteError(new IOException("Failed to mkdirs " + directory), directory);
      }
   }

   public static boolean delete(String file) {
      File f = new File(file);
      return f.delete();
   }

   public static void delete(File... files) {
      File[] var1 = files;
      int var2 = files.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         File file = var1[var3];
         file.delete();
      }

   }

   public static void deleteAsync(final String file) {
      Runnable runnable = new Runnable() {
         public void run() {
            FileUtils.deleteWithConfirm(new File(file));
         }
      };
      ScheduledExecutors.nonPeriodicTasks.execute(runnable);
   }

   public static String stringifyFileSize(double value) {
      double d;
      String val;
      if(value >= 1.099511627776E12D) {
         d = value / 1.099511627776E12D;
         val = df.format(d);
         return val + " TiB";
      } else if(value >= 1.073741824E9D) {
         d = value / 1.073741824E9D;
         val = df.format(d);
         return val + " GiB";
      } else if(value >= 1048576.0D) {
         d = value / 1048576.0D;
         val = df.format(d);
         return val + " MiB";
      } else if(value >= 1024.0D) {
         d = value / 1024.0D;
         val = df.format(d);
         return val + " KiB";
      } else {
         val = df.format(value);
         return val + " bytes";
      }
   }

   public static void deleteRecursive(File dir) {
      if(dir.isDirectory()) {
         String[] children = dir.list();
         String[] var2 = children;
         int var3 = children.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String child = var2[var4];
            deleteRecursive(new File(dir, child));
         }
      }

      deleteWithConfirm(dir);
   }

   public static void deleteRecursiveOnExit(File dir) {
      if(dir.isDirectory()) {
         String[] children = dir.list();
         String[] var2 = children;
         int var3 = children.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String child = var2[var4];
            deleteRecursiveOnExit(new File(dir, child));
         }
      }

      logger.trace("Scheduling deferred deletion of file: {}", dir);
      dir.deleteOnExit();
   }

   public static void handleCorruptSSTable(CorruptSSTableException e) {
      JVMStabilityInspector.inspectThrowable(e);
   }

   public static void handleFSError(FSError e) {
      JVMStabilityInspector.inspectThrowable(e);
   }

   public static long folderSize(File folder) {
      final long[] sizeArr = new long[]{0L};

      try {
         Files.walkFileTree(folder.toPath(), new SimpleFileVisitor<Path>() {
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
               sizeArr[0] += attrs.size();
               return FileVisitResult.CONTINUE;
            }
         });
      } catch (IOException var3) {
         logger.error("Error while getting {} folder size. {}", folder, var3.getMessage());
      }

      return sizeArr[0];
   }

   public static void copyTo(DataInput in, OutputStream out, int length) throws IOException {
      byte[] buffer = new byte[65536];

      int copiedBytes;
      for(copiedBytes = 0; copiedBytes + buffer.length < length; copiedBytes += buffer.length) {
         in.readFully(buffer);
         out.write(buffer);
      }

      if(copiedBytes < length) {
         int left = length - copiedBytes;
         in.readFully(buffer, 0, left);
         out.write(buffer, 0, left);
      }

   }

   public static boolean isSubDirectory(File parent, File child) throws IOException {
      parent = parent.getCanonicalFile();
      child = child.getCanonicalFile();

      for(File toCheck = child; toCheck != null; toCheck = toCheck.getParentFile()) {
         if(parent.equals(toCheck)) {
            return true;
         }
      }

      return false;
   }

   public static void append(File file, String... lines) {
      if(file.exists()) {
         write(file, Arrays.asList(lines), new StandardOpenOption[]{StandardOpenOption.APPEND});
      } else {
         write(file, Arrays.asList(lines), new StandardOpenOption[]{StandardOpenOption.CREATE});
      }

   }

   public static void appendAndSync(File file, String... lines) {
      if(file.exists()) {
         write(file, Arrays.asList(lines), new StandardOpenOption[]{StandardOpenOption.APPEND, StandardOpenOption.SYNC});
      } else {
         write(file, Arrays.asList(lines), new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.SYNC});
      }

   }

   public static void replace(File file, String... lines) {
      write(file, Arrays.asList(lines), new StandardOpenOption[]{StandardOpenOption.TRUNCATE_EXISTING});
   }

   public static void write(File file, List<String> lines, StandardOpenOption... options) {
      try {
         Files.write(file.toPath(), lines, CHARSET, options);
      } catch (IOException var4) {
         throw new FSWriteError(var4, file);
      }
   }

   public static String readLine(File file) {
      List<String> lines = readLines(file);
      return lines != null && lines.size() >= 1?(String)lines.get(0):null;
   }

   public static List<String> readLines(File file) {
      try {
         return Files.readAllLines(file.toPath(), CHARSET);
      } catch (IOException var2) {
         if(var2 instanceof NoSuchFileException) {
            return Collections.emptyList();
         } else {
            throw new RuntimeException(var2);
         }
      }
   }

   public static int getIOGlobalQueueDepth() {
      return ((Integer)FileUtils.MountPoint.mountPoints.values().stream().map((mountPoint) -> {
         return Integer.valueOf(mountPoint.ioQueueDepth);
      }).reduce(Math::min).orElse(Integer.valueOf(FileUtils.MountPoint.DEFAULT.ioQueueDepth))).intValue();
   }

   private static boolean checkRamdisk(String fstype) {
      return "tmpfs".equals(fstype);
   }

   public static String detectVirtualization() {
      return !FBUtilities.isLinux?null:detectVirtualization(() -> {
         return FBUtilities.execBlocking(CPUID_COMMANDLINE, 1, TimeUnit.SECONDS);
      }, () -> {
         File hypervisorCaps = new File("/sys/hypervisor/properties/capabilities");
         return hypervisorCaps.exists() && hypervisorCaps.canRead()?readLines(hypervisorCaps):null;
      }, () -> {
         return (new File("/sys/bus/xen/devices/")).listFiles();
      }, FBUtilities.CpuInfo::load, (filter) -> {
         return (new File("/dev/disk/by-id")).listFiles((n) -> {
            return n.getName().contains(filter);
         });
      });
   }

   @VisibleForTesting
   static String detectVirtualization(Supplier<String> cpuid, Supplier<List<String>> hypervisorCaps, Supplier<File[]> xenDevices, Supplier<FBUtilities.CpuInfo> cpuInfoSupplier, Function<String, File[]> disksByIdFilter) {
      try {
         try {
            String cpuidOutput = (String)cpuid.get();
            String[] cpuidLines = cpuidOutput.split("\n");
            String vendorId = cpuIdVendorId(cpuidLine(cpuidLines, "0x00000000", "0x00"));
            String virtByVendorId = virtualizationFromVendorId(vendorId);
            if(virtByVendorId != null) {
               return virtByVendorId;
            }

            int[] cpuidLeaf1 = cpuIdParse(cpuidLine(cpuidLines, "0x00000001", "0x00"));
            boolean hypervisorFlag = (cpuidLeaf1[2] & -2147483648) != 0;
            if(hypervisorFlag) {
               return "unknown (CPUID leaf 1 ECX bit 31 set)";
            }

            boolean hypervisorLeafsPresent = Arrays.stream(cpuidLines).anyMatch((s) -> {
               return s.startsWith("   0x400000");
            });
            if(hypervisorLeafsPresent) {
               return "unknown (CPUID hypervisor leafs)";
            }
         } catch (Exception var13) {
            if(logger.isTraceEnabled()) {
               logger.trace("Unable to parse CPUID information. This is usually because 'cpuid' tool is not installed on the system.", var13);
            } else {
               logger.debug("Unable to parse CPUID information ({}). This is usually because 'cpuid' tool is not installed on the system.", var13.toString());
            }
         }

         List<String> hCaps = (List)hypervisorCaps.get();
         if(hCaps != null) {
            Set<String> caps = (Set)((List)hypervisorCaps.get()).stream().flatMap((s) -> {
               return Arrays.stream(s.split(" "));
            }).map((s) -> {
               return s.split("-")[0];
            }).collect(Collectors.toSet());
            if(caps.contains("xen") && caps.contains("hvm")) {
               return "Xen HVM";
            }

            if(caps.contains("xen")) {
               return "Xen";
            }

            if(!caps.isEmpty()) {
               return "unknwon (hypervisor capabilities)";
            }
         }

         File[] xenDevs = (File[])xenDevices.get();
         if(xenDevs != null && xenDevs.length > 0) {
            return "Xen";
         }

         File[] disks = (File[])disksByIdFilter.apply("-QEMU_");
         if(disks != null && disks.length > 0) {
            return "Xen/KVM";
         }

         disks = (File[])disksByIdFilter.apply("-VBOX_");
         if(disks != null && disks.length > 0) {
            return "VirtualBox";
         }

         try {
            FBUtilities.CpuInfo cpuInfo = (FBUtilities.CpuInfo)cpuInfoSupplier.get();
            if(!cpuInfo.getProcessors().isEmpty()) {
               FBUtilities.CpuInfo.PhysicalProcessor processor = (FBUtilities.CpuInfo.PhysicalProcessor)cpuInfo.getProcessors().get(0);
               String virtByVendorId = virtualizationFromVendorId(processor.getVendorId());
               if(virtByVendorId != null) {
                  return virtByVendorId;
               }

               if(processor.hasFlag("hypervisor")) {
                  return "unknown (hypervisor CPU flag present)";
               }
            }
         } catch (Exception var12) {
            ;
         }
      } catch (Exception var14) {
         if(logger.isTraceEnabled()) {
            logger.warn("Unable to detect virtualization/hypervisor", var14);
         } else {
            logger.warn("Unable to detect virtualization/hypervisor");
         }
      }

      return null;
   }

   private static String virtualizationFromVendorId(String vendorId) {
      byte var2 = -1;
      switch(vendorId.hashCode()) {
      case -2097746820:
         if(vendorId.equals("bhyve bhyve ")) {
            var2 = 14;
         }
         break;
      case -2084343424:
         if(vendorId.equals("KVMKVMKVMKVM")) {
            var2 = 15;
         }
         break;
      case -2049933321:
         if(vendorId.equals("CyrixInstead")) {
            var2 = 4;
         }
         break;
      case -2026633413:
         if(vendorId.equals("RiseRiseRise")) {
            var2 = 9;
         }
         break;
      case -1573944549:
         if(vendorId.equals("GenuineIntel")) {
            var2 = 1;
         }
         break;
      case -1564766526:
         if(vendorId.equals("GenuineTMx86")) {
            var2 = 6;
         }
         break;
      case -1446031617:
         if(vendorId.equals("UMC UMC UMC ")) {
            var2 = 11;
         }
         break;
      case -916705064:
         if(vendorId.equals(" lrpepyh vr")) {
            var2 = 17;
         }
         break;
      case -757482392:
         if(vendorId.equals("VMwareVMware")) {
            var2 = 18;
         }
         break;
      case -385325293:
         if(vendorId.equals("NexGenDriven")) {
            var2 = 8;
         }
         break;
      case -306274326:
         if(vendorId.equals("XenVMMXenVMM")) {
            var2 = 19;
         }
         break;
      case -59438629:
         if(vendorId.equals("TransmetaCPU")) {
            var2 = 5;
         }
         break;
      case -39539645:
         if(vendorId.equals("Geode by NSC")) {
            var2 = 7;
         }
         break;
      case 104479446:
         if(vendorId.equals("VIA VIA VIA ")) {
            var2 = 12;
         }
         break;
      case 229936431:
         if(vendorId.equals("AMDisbetter!")) {
            var2 = 3;
         }
         break;
      case 948026061:
         if(vendorId.equals("CentaurHauls")) {
            var2 = 2;
         }
         break;
      case 1019203817:
         if(vendorId.equals("SiS SiS SiS ")) {
            var2 = 10;
         }
         break;
      case 1189633907:
         if(vendorId.equals("Vortex86 SoC")) {
            var2 = 13;
         }
         break;
      case 1885404667:
         if(vendorId.equals("AuthenticAMD")) {
            var2 = 0;
         }
         break;
      case 1919407552:
         if(vendorId.equals("Microsoft Hv")) {
            var2 = 16;
         }
      }

      switch(var2) {
      case 0:
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
      case 7:
      case 8:
      case 9:
      case 10:
      case 11:
      case 12:
      case 13:
         return null;
      case 14:
         return "bhyve";
      case 15:
         return "KVM";
      case 16:
         return "MS Hyper-V";
      case 17:
         return "Parallels";
      case 18:
         return "VMWare";
      case 19:
         return "Xen HVM";
      default:
         return vendorId + " (unknown vendor in CPUID)";
      }
   }

   private static String cpuidLine(String[] cpuidLines, String leaf, String ecx) {
      return (String)Arrays.stream(cpuidLines).map(String::trim).filter((s) -> {
         return s.startsWith(leaf + ' ' + ecx + ':');
      }).findFirst().orElse((Object)null);
   }

   static int[] cpuIdParse(String cpuidLine) {
      StringTokenizer vendorIdLine = new StringTokenizer(cpuidLine);
      vendorIdLine.nextToken();
      vendorIdLine.nextToken();
      int eax = (int)Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
      int ebx = (int)Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
      int ecx = (int)Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
      int edx = (int)Long.parseLong(vendorIdLine.nextToken().substring(6), 16);
      return new int[]{eax, ebx, ecx, edx};
   }

   static String cpuIdVendorId(String cpuidLine) throws IOException {
      int[] registers = cpuIdParse(cpuidLine);
      byte[] bytes = new byte[12];
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.putInt(registers[1]);
      bb.putInt(registers[3]);
      bb.putInt(registers[2]);
      return new String(bytes, StandardCharsets.UTF_8);
   }

   public static long getTotalSpace(File file) {
      return handleLargeFileSystem(file.getTotalSpace());
   }

   public static long getFreeSpace(File file) {
      return handleLargeFileSystem(file.getFreeSpace());
   }

   public static long getUsableSpace(File file) {
      return handleLargeFileSystem(file.getUsableSpace());
   }

   public static FileStore getFileStore(Path path) throws IOException {
      return new FileUtils.SafeFileStore(Files.getFileStore(path));
   }

   private static long handleLargeFileSystem(long size) {
      return size < 0L?9223372036854775807L:size;
   }

   private FileUtils() {
   }

   static {
      CHARSET = StandardCharsets.UTF_8;
      logger = LoggerFactory.getLogger(FileUtils.class);
      df = new DecimalFormat("#.##");
      CPUID_COMMANDLINE = new String[]{"cpuid", "-1", "-r"};
      boolean canClean = false;

      try {
         ByteBuffer buf = ByteBuffer.allocateDirect(1);
         ((DirectBuffer)buf).cleaner().clean();
         canClean = true;
      } catch (Throwable var2) {
         JVMStabilityInspector.inspectThrowable(var2);
         logger.info("Cannot initialize un-mmaper.  (Are you using a non-Oracle JVM?)  Compacted data files will not be removed promptly.  Consider using an Oracle JVM or using standard disk access mode");
      }

      isCleanerAvailable = canClean;
   }

   private static final class SafeFileStore extends FileStore {
      private final FileStore fileStore;

      public SafeFileStore(FileStore fileStore) {
         this.fileStore = fileStore;
      }

      public String name() {
         return this.fileStore.name();
      }

      public String type() {
         return this.fileStore.type();
      }

      public boolean isReadOnly() {
         return this.fileStore.isReadOnly();
      }

      public long getTotalSpace() throws IOException {
         return FileUtils.handleLargeFileSystem(this.fileStore.getTotalSpace());
      }

      public long getUsableSpace() throws IOException {
         return FileUtils.handleLargeFileSystem(this.fileStore.getUsableSpace());
      }

      public long getUnallocatedSpace() throws IOException {
         return FileUtils.handleLargeFileSystem(this.fileStore.getUnallocatedSpace());
      }

      public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
         return this.fileStore.supportsFileAttributeView(type);
      }

      public boolean supportsFileAttributeView(String name) {
         return this.fileStore.supportsFileAttributeView(name);
      }

      public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
         return this.fileStore.getFileStoreAttributeView(type);
      }

      public Object getAttribute(String attribute) throws IOException {
         return this.fileStore.getAttribute(attribute);
      }
   }

   public static class MountPoint {
      private static final Supplier<String> defaultSectorSize = () -> {
         return System.getProperty("dse.io.default.sector.size", "512");
      };
      private static final Supplier<String> defaultIOQueueDepth = () -> {
         return System.getProperty("dse.io.default.queue.depth", "128");
      };
      public static final FileUtils.MountPoint DEFAULT;
      private static final Map<Path, FileUtils.MountPoint> mountPoints;
      public final Path mountpoint;
      public final String deviceMajorMinor;
      public final String device;
      public final String fstype;
      public final boolean blockDevice;
      public final boolean onSSD;
      public final int sectorSize;
      public final int ioQueueDepth;

      private static Map<Path, FileUtils.MountPoint> getDiskPartitions() {
         if(!FBUtilities.isLinux) {
            return Collections.emptyMap();
         } else {
            try {
               Reader source = new InputStreamReader(new FileInputStream("/proc/self/mountinfo"), "UTF-8");
               Throwable var1 = null;

               Map var2;
               try {
                  var2 = getDiskPartitions(source);
               } catch (Throwable var12) {
                  var1 = var12;
                  throw var12;
               } finally {
                  if(source != null) {
                     if(var1 != null) {
                        try {
                           source.close();
                        } catch (Throwable var11) {
                           var1.addSuppressed(var11);
                        }
                     } else {
                        source.close();
                     }
                  }

               }

               return var2;
            } catch (Throwable var14) {
               FileUtils.logger.error("Failed to retrieve disk partitions", var14);
               return Collections.emptyMap();
            }
         }
      }

      @VisibleForTesting
      static Map<Path, FileUtils.MountPoint> getDiskPartitions(Reader source) throws IOException {
         assert FBUtilities.isLinux;

         Map<Path, FileUtils.MountPoint> dirToDisk = new TreeMap((a, b) -> {
            int cmp = -Integer.compare(a.getNameCount(), b.getNameCount());
            return cmp != 0?cmp:a.toString().compareTo(b.toString());
         });
         BufferedReader bufferedReader = new BufferedReader(source);
         Throwable var3 = null;

         try {
            String line;
            try {
               while((line = bufferedReader.readLine()) != null) {
                  String[] parts = line.split(" ");
                  String deviceMajorMinor = parts[2];
                  String mountPoint = parts[4];

                  int opt;
                  for(opt = 0; opt < parts.length && !"-".equals(parts[opt]); ++opt) {
                     ;
                  }

                  String fstype = parts[opt + 1];
                  String device = parts[opt + 2];
                  Path mp = Paths.get(mountPoint, new String[0]);
                  dirToDisk.put(mp, new FileUtils.MountPoint(mp, deviceMajorMinor, device, fstype));
               }
            } catch (Throwable var19) {
               var3 = var19;
               throw var19;
            }
         } finally {
            if(bufferedReader != null) {
               if(var3 != null) {
                  try {
                     bufferedReader.close();
                  } catch (Throwable var18) {
                     var3.addSuppressed(var18);
                  }
               } else {
                  bufferedReader.close();
               }
            }

         }

         return dirToDisk;
      }

      MountPoint(Path mountpoint, String deviceMajorMinor, String device, String fstype) throws IOException {
         this(mountpoint, deviceMajorMinor, device, fstype, getQueueFolder(deviceMajorMinor));
      }

      private MountPoint(Path mountpoint, String deviceMajorMinor, String device, String fstype, Path queueFolder) throws IOException {
         this(mountpoint, deviceMajorMinor, device, fstype, queueFolder != null, FileUtils.checkRamdisk(fstype)?true:isPartitionOnSSD(queueFolder), FileUtils.checkRamdisk(fstype)?DEFAULT.sectorSize:getSectorSize(queueFolder, device), FileUtils.checkRamdisk(fstype)?DEFAULT.ioQueueDepth:getMaxQueueDepth(queueFolder, device));
      }

      private MountPoint(Path mountpoint, String deviceMajorMinor, String device, String fstype, boolean blockDevice, boolean onSSD, int sectorSize, int ioQueueDepth) {
         this.mountpoint = mountpoint;
         this.deviceMajorMinor = deviceMajorMinor;
         this.device = device;
         this.fstype = fstype;
         this.blockDevice = blockDevice;
         this.onSSD = onSSD;
         this.sectorSize = sectorSize;
         this.ioQueueDepth = ioQueueDepth;
      }

      private static Path getQueueFolder(String deviceMajorMinor) throws IOException {
         assert FBUtilities.isLinux;

         Path sysDevBlock = Paths.get("/sys/dev/block", new String[]{deviceMajorMinor});
         if(!sysDevBlock.toFile().isDirectory()) {
            return null;
         } else {
            Path partition = sysDevBlock.toRealPath(new LinkOption[0]);
            boolean isPartition = partition.resolve("partition").toFile().exists();
            Path device = isPartition?partition.resolve("..").toRealPath(new LinkOption[0]):partition;
            if(!device.toFile().isDirectory()) {
               return null;
            } else {
               Path queue = device.resolve("queue");
               return queue.toFile().isDirectory()?queue:null;
            }
         }
      }

      private static boolean isPartitionOnSSD(Path queue) throws IOException {
         assert FBUtilities.isLinux;

         if(queue == null) {
            return false;
         } else {
            Path rotational = queue.resolve("rotational");
            return !rotational.toFile().exists()?false:"0".equals(Files.lines(rotational).findFirst().orElse("1"));
         }
      }

      private static int getSectorSize(Path queue, String deviceName) throws IOException {
         assert FBUtilities.isLinux;

         int operatorSectorSize = Integer.parseInt(System.getProperty(String.format("dse.io.%s.sector.size", new Object[]{deviceName}), "0"));
         if(operatorSectorSize > 0) {
            FileUtils.logger.info("Using operator sector size {} for {}", Integer.valueOf(operatorSectorSize), deviceName);
            return operatorSectorSize;
         } else if(queue == null) {
            return Integer.parseInt((String)defaultSectorSize.get());
         } else {
            Path logical_block_size = queue.resolve("logical_block_size");
            if(!logical_block_size.toFile().exists()) {
               FileUtils.logger.warn("Could not determine sector size for {}, assuming {}", deviceName, defaultSectorSize.get());
               return Integer.parseInt((String)defaultSectorSize.get());
            } else {
               return Integer.parseInt((String)Files.lines(logical_block_size).findFirst().orElseGet(defaultSectorSize));
            }
         }
      }

      private static int getMaxQueueDepth(Path queue, String deviceName) throws IOException {
         assert FBUtilities.isLinux;

         if(queue == null) {
            return Integer.parseInt((String)defaultIOQueueDepth.get());
         } else {
            Path logical_block_size = queue.resolve("nr_requests");
            if(!logical_block_size.toFile().exists()) {
               FileUtils.logger.warn("Could not determine the IO queue depth for {}, assuming {}", deviceName, defaultIOQueueDepth.get());
               return Integer.parseInt((String)defaultIOQueueDepth.get());
            } else {
               return Integer.parseInt((String)Files.lines(logical_block_size).findFirst().orElseGet(defaultIOQueueDepth));
            }
         }
      }

      public static FileUtils.MountPoint mountPointForDirectory(String dir) {
         if(mountPoints.isEmpty()) {
            return DEFAULT;
         } else {
            try {
               Path path = Paths.get(dir, new String[0]).toAbsolutePath();
               Iterator var2 = mountPoints.entrySet().iterator();

               Entry entry;
               do {
                  if(!var2.hasNext()) {
                     FileUtils.logger.debug("Could not find mount-point for {}", dir);
                     return DEFAULT;
                  }

                  entry = (Entry)var2.next();
               } while(!path.startsWith((Path)entry.getKey()));

               return (FileUtils.MountPoint)entry.getValue();
            } catch (Throwable var4) {
               FileUtils.logger.debug("Failed to detect mount point for directory {}: {}", dir, var4.getMessage());
               return DEFAULT;
            }
         }
      }

      public static boolean hasMountPoints() {
         return !mountPoints.isEmpty();
      }

      public String toString() {
         return "MountPoint{mountpoint=" + this.mountpoint + ", device='" + this.device + '\'' + ", majorMinor=" + this.deviceMajorMinor + ", fstype=" + this.fstype + ", blockDevice=" + this.blockDevice + ", onSSD=" + this.onSSD + ", sectorSize=" + this.sectorSize + ", ioQueueDepth=" + this.ioQueueDepth + '}';
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            FileUtils.MountPoint that = (FileUtils.MountPoint)o;
            return this.blockDevice == that.blockDevice && this.onSSD == that.onSSD && this.sectorSize == that.sectorSize && this.ioQueueDepth == that.ioQueueDepth && Objects.equals(this.mountpoint, that.mountpoint) && Objects.equals(this.deviceMajorMinor, that.deviceMajorMinor) && Objects.equals(this.device, that.device) && Objects.equals(this.fstype, that.fstype);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.mountpoint, this.device, this.deviceMajorMinor, this.fstype, Boolean.valueOf(this.blockDevice), Boolean.valueOf(this.onSSD), Integer.valueOf(this.sectorSize), Integer.valueOf(this.ioQueueDepth)});
      }

      static {
         DEFAULT = new FileUtils.MountPoint(Paths.get(".", new String[0]).getRoot(), (String)null, (String)null, (String)null, true, true, Integer.parseInt((String)defaultSectorSize.get()), Integer.parseInt((String)defaultIOQueueDepth.get()));
         mountPoints = getDiskPartitions();
      }
   }
}
