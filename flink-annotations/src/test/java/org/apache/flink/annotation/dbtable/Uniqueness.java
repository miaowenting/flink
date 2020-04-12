package org.apache.flink.annotation.dbtable;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
public @interface Uniqueness {
	Constraints constraints() default @Constraints(unique = true);
}
