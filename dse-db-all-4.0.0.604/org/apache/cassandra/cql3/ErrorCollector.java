package org.apache.cassandra.cql3;

import java.util.LinkedList;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.exceptions.SyntaxException;

public final class ErrorCollector implements ErrorListener {
   private static final int FIRST_TOKEN_OFFSET = 10;
   private static final int LAST_TOKEN_OFFSET = 2;
   private final String query;
   private final LinkedList<String> errorMsgs = new LinkedList();

   public ErrorCollector(String query) {
      this.query = query;
   }

   public void syntaxError(BaseRecognizer recognizer, String[] tokenNames, RecognitionException e) {
      String hdr = recognizer.getErrorHeader(e);
      String msg = recognizer.getErrorMessage(e, tokenNames);
      StringBuilder builder = (new StringBuilder()).append(hdr).append(' ').append(msg);
      if(recognizer instanceof Parser) {
         this.appendQuerySnippet((Parser)recognizer, builder);
      }

      this.errorMsgs.add(builder.toString());
   }

   public void syntaxError(BaseRecognizer recognizer, String errorMsg) {
      this.errorMsgs.add(errorMsg);
   }

   public void throwFirstSyntaxError() throws SyntaxException {
      if(!this.errorMsgs.isEmpty()) {
         throw new SyntaxException((String)this.errorMsgs.getFirst());
      }
   }

   private void appendQuerySnippet(Parser parser, StringBuilder builder) {
      TokenStream tokenStream = parser.getTokenStream();
      int index = tokenStream.index();
      int size = tokenStream.size();
      Token from = tokenStream.get(getSnippetFirstTokenIndex(index));
      Token to = tokenStream.get(getSnippetLastTokenIndex(index, size));
      Token offending = tokenStream.get(getOffendingTokenIndex(index, size));
      this.appendSnippet(builder, from, to, offending);
   }

   final void appendSnippet(StringBuilder builder, Token from, Token to, Token offending) {
      if(areTokensValid(new Token[]{from, to, offending})) {
         String[] lines = this.query.split("\n");
         boolean includeQueryStart = from.getLine() == 1 && from.getCharPositionInLine() == 0;
         boolean includeQueryEnd = to.getLine() == lines.length && getLastCharPositionInLine(to) == lines[lines.length - 1].length();
         builder.append(" (");
         if(!includeQueryStart) {
            builder.append("...");
         }

         String toLine = lines[lineIndex(to)];
         int toEnd = getLastCharPositionInLine(to);
         lines[lineIndex(to)] = toEnd >= toLine.length()?toLine:toLine.substring(0, toEnd);
         lines[lineIndex(offending)] = highlightToken(lines[lineIndex(offending)], offending);
         lines[lineIndex(from)] = lines[lineIndex(from)].substring(from.getCharPositionInLine());
         int i = lineIndex(from);

         for(int m = lineIndex(to); i <= m; ++i) {
            builder.append(lines[i]);
         }

         if(!includeQueryEnd) {
            builder.append("...");
         }

         builder.append(")");
      }
   }

   private static boolean areTokensValid(Token... tokens) {
      Token[] var1 = tokens;
      int var2 = tokens.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         Token token = var1[var3];
         if(!isTokenValid(token)) {
            return false;
         }
      }

      return true;
   }

   private static boolean isTokenValid(Token token) {
      return token.getLine() > 0 && token.getCharPositionInLine() >= 0;
   }

   private static int getOffendingTokenIndex(int index, int size) {
      return Math.min(index, size - 1);
   }

   private static String highlightToken(String line, Token token) {
      String newLine = insertChar(line, getLastCharPositionInLine(token), ']');
      return insertChar(newLine, token.getCharPositionInLine(), '[');
   }

   private static int getLastCharPositionInLine(Token token) {
      return token.getCharPositionInLine() + getLength(token);
   }

   private static int getLength(Token token) {
      return token.getText().length();
   }

   private static String insertChar(String s, int index, char c) {
      return s.substring(0, index) + c + s.substring(index);
   }

   private static int lineIndex(Token token) {
      return token.getLine() - 1;
   }

   private static int getSnippetLastTokenIndex(int index, int size) {
      return Math.min(size - 1, index + 2);
   }

   private static int getSnippetFirstTokenIndex(int index) {
      return Math.max(0, index - 10);
   }
}
