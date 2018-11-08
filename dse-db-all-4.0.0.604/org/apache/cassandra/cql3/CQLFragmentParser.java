package org.apache.cassandra.cql3;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.exceptions.SyntaxException;

public final class CQLFragmentParser {
   public CQLFragmentParser() {
   }

   public static <R> R parseAny(CQLFragmentParser.CQLParserFunction<R> parserFunction, String input, String meaning) {
      try {
         return parseAnyUnhandled(parserFunction, input);
      } catch (RuntimeException var4) {
         throw new SyntaxException(String.format("Failed parsing %s: [%s] reason: %s %s", new Object[]{meaning, input, var4.getClass().getSimpleName(), var4.getMessage()}));
      } catch (RecognitionException var5) {
         throw new SyntaxException("Invalid or malformed " + meaning + ": " + var5.getMessage());
      }
   }

   public static <R> R parseAnyUnhandled(CQLFragmentParser.CQLParserFunction<R> parserFunction, String input) throws RecognitionException {
      ErrorCollector errorCollector = new ErrorCollector(input);
      CharStream stream = new ANTLRStringStream(input);
      CqlLexer lexer = new CqlLexer(stream);
      lexer.addErrorListener(errorCollector);
      TokenStream tokenStream = new CommonTokenStream(lexer);
      CqlParser parser = new CqlParser(tokenStream);
      parser.addErrorListener(errorCollector);
      R r = parserFunction.parse(parser);
      errorCollector.throwFirstSyntaxError();
      return r;
   }

   @FunctionalInterface
   public interface CQLParserFunction<R> {
      R parse(CqlParser var1) throws RecognitionException;
   }
}
