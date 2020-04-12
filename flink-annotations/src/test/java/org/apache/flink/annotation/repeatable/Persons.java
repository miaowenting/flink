package org.apache.flink.annotation.repeatable;

/**
 * Description:
 *  容器注解，必须要有一个value属性
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
public @interface Persons {
	Person[] value();
}
