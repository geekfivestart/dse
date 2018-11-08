package com.datastax.bdp.util;

import java.util.Optional;
import java.util.regex.Pattern;

public class StringUtil {
   private static final Pattern nonPrintablePattern = Pattern.compile("\\P{Print}");

   public StringUtil() {
   }

   public static String stripNonPrintableCharacters(String input) {
      return input == null?null:nonPrintablePattern.matcher(input).replaceAll("");
   }

   public static Optional<String> stringToOptional(String string) {
      return string != null && !string.trim().isEmpty()?Optional.of(string):Optional.empty();
   }
}
