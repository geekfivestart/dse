package com.datastax.bdp.util.rpc;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.apache.cassandra.auth.permission.CorePermission;

@Retention(RetentionPolicy.RUNTIME)
public @interface Rpc {
   String name();

   CorePermission permission();

   boolean multiRow() default false;
}
