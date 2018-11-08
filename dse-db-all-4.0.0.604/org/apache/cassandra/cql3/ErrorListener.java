package org.apache.cassandra.cql3;

import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.RecognitionException;

public interface ErrorListener {
   void syntaxError(BaseRecognizer var1, String[] var2, RecognitionException var3);

   void syntaxError(BaseRecognizer var1, String var2);
}
