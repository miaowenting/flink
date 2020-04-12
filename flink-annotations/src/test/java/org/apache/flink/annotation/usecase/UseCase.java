package org.apache.flink.annotation.usecase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface UseCase {
	/**
	 * 用例id
 	 */
	int id();

	/**
	 * 用例描述
	 */
	String description() default "no description";
}
