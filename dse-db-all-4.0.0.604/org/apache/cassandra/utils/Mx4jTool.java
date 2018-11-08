package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mx4jTool {
   private static final Logger logger = LoggerFactory.getLogger(Mx4jTool.class);

   public Mx4jTool() {
   }

   public static boolean maybeLoad() {
      try {
         logger.trace("Will try to load mx4j now, if it's in the classpath");
         MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
         ObjectName processorName = new ObjectName("Server:name=XSLTProcessor");
         Class<?> httpAdaptorClass = Class.forName("mx4j.tools.adaptor.http.HttpAdaptor");
         Object httpAdaptor = httpAdaptorClass.newInstance();
         httpAdaptorClass.getMethod("setHost", new Class[]{String.class}).invoke(httpAdaptor, new Object[]{getAddress()});
         httpAdaptorClass.getMethod("setPort", new Class[]{Integer.TYPE}).invoke(httpAdaptor, new Object[]{Integer.valueOf(getPort())});
         ObjectName httpName = new ObjectName("system:name=http");
         mbs.registerMBean(httpAdaptor, httpName);
         Class<?> xsltProcessorClass = Class.forName("mx4j.tools.adaptor.http.XSLTProcessor");
         Object xsltProcessor = xsltProcessorClass.newInstance();
         httpAdaptorClass.getMethod("setProcessor", new Class[]{Class.forName("mx4j.tools.adaptor.http.ProcessorMBean")}).invoke(httpAdaptor, new Object[]{xsltProcessor});
         mbs.registerMBean(xsltProcessor, processorName);
         httpAdaptorClass.getMethod("start", new Class[0]).invoke(httpAdaptor, new Object[0]);
         logger.info("mx4j successfuly loaded");
         return true;
      } catch (ClassNotFoundException var7) {
         logger.trace("Will not load MX4J, mx4j-tools.jar is not in the classpath");
      } catch (Exception var8) {
         logger.warn("Could not start register mbean in JMX", var8);
      }

      return false;
   }

   private static String getAddress() {
      String sAddress = System.getProperty("mx4jaddress");
      if(StringUtils.isEmpty(sAddress)) {
         sAddress = FBUtilities.getBroadcastAddress().getHostAddress();
      }

      return sAddress;
   }

   private static int getPort() {
      int port = 8081;
      String sPort = System.getProperty("mx4jport");
      if(StringUtils.isNotEmpty(sPort)) {
         port = Integer.parseInt(sPort);
      }

      return port;
   }
}
