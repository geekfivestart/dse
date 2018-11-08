package org.apache.cassandra.tools;

import org.apache.cassandra.utils.FBUtilities;

public class GetVersion {
   public GetVersion() {
   }

   public static void main(String[] args) {
      System.out.println(FBUtilities.getReleaseVersionString());
   }
}
