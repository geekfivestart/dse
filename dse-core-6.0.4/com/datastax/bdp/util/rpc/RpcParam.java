package com.datastax.bdp.util.rpc;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RpcParam {
   String name();

   String help() default "";
}
