package com.datastax.bdp.server;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerId {
   private static final Logger logger = LoggerFactory.getLogger(ServerId.class);

   public ServerId() {
   }

   public static String genID() {
      try {
         int lowestIndex = 2147483647;
         String macAddress = "";
         Enumeration ifaces = NetworkInterface.getNetworkInterfaces();

         while(true) {
            int ifaceIndex;
            byte[] mac;
            do {
               NetworkInterface iface;
               do {
                  do {
                     do {
                        if(!ifaces.hasMoreElements()) {
                           return macAddress;
                        }

                        iface = (NetworkInterface)ifaces.nextElement();
                        ifaceIndex = iface.getIndex();
                     } while(ifaceIndex >= lowestIndex);
                  } while(iface.isLoopback());
               } while(iface.isVirtual());

               mac = iface.getHardwareAddress();
            } while(mac == null);

            StringBuilder sb = new StringBuilder();

            for(int i = 0; i < mac.length; ++i) {
               sb.append(String.format("%02X%s", new Object[]{Byte.valueOf(mac[i]), i < mac.length - 1?"-":""}));
            }

            lowestIndex = ifaceIndex;
            macAddress = sb.toString();
         }
      } catch (SocketException var8) {
         throw new RuntimeException(var8);
      }
   }
}
