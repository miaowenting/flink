package org.apache.flink.annotation;

/**
 * Description:
 * 注解的应用。
 * 给注解的属性赋值，赋值的方式是在注解的括号内以 value="" 形式，多个属性之前用 ，隔开。
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
@TestAnnotation("defaultValue")
public class Test {

	public static void main(String[] args) {
		// 注解通过反射获取，通过 Class 对象的 isAnnotationPresent() 方法判断它是否应用了某个注解
		boolean hasAnnotation = Test.class.isAnnotationPresent(TestAnnotation.class);
		if (hasAnnotation) {
			// 通过 getAnnotation() 方法来获取 Annotation 对象
			TestAnnotation testAnnotation = Test.class.getAnnotation(TestAnnotation.class);
			System.out.println("id:" + testAnnotation.id());
			System.out.println("msg:" + testAnnotation.msg());
		}

	}
}

