package com.datastax.bdp.tools;

public interface Command {
   String getName();

   String getHelp();

   String getOptionsHelp();
}
