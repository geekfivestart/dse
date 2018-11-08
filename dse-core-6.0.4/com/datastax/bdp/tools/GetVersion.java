package com.datastax.bdp.tools;

import com.datastax.bdp.server.SystemInfo;

public class GetVersion {
   public GetVersion() {
   }

   public static void main(String[] args) {
      System.out.println(getReleaseVersionString());
   }

   public static String getReleaseVersionString() {
      try {
         return SystemInfo.getReleaseVersion("dse_version");
      } catch (Exception var1) {
         return "debug version";
      }
   }
}
