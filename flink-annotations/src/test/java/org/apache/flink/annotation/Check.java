package org.apache.flink.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Check {
	String value();
}
