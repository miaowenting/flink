package org.apache.flink.annotation.repeatable;

import java.lang.annotation.Repeatable;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
@Repeatable(Persons.class)
public @interface Person {
	String role();
}
