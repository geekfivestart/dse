package org.apache.cassandra.io.util;

import java.io.IOException;

public interface ThrowingFunction<ArgType, ResType> {
   ResType apply(ArgType var1) throws IOException;
}
