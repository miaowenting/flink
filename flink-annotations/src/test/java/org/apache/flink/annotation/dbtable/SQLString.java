package org.apache.flink.annotation.dbtable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
@Target(value = ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SQLString {

    int len() default 0;
    String name() default "";
    Constraints constraints() default @Constraints;
}
