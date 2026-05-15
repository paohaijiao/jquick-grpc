package com.github.paohaijiao.grpc.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface JQuickGrpcService {

    String name() default "";

    int version() default 1;

    boolean enableHealthCheck() default true;
}
