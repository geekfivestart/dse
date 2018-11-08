package org.apache.cassandra.index.sasi.analyzer;

import java.io.IOException;
import java.io.Reader;

public interface StandardTokenizerInterface {
   String getText();

   char[] getArray();

   byte[] getBytes();

   int yychar();

   int yylength();

   int getNextToken() throws IOException;

   void yyreset(Reader var1);
}
