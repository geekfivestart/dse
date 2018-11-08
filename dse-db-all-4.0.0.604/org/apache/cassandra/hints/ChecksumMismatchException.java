package org.apache.cassandra.hints;

import java.io.IOException;

final class ChecksumMismatchException extends IOException {
   ChecksumMismatchException(String message) {
      super(message);
   }
}
