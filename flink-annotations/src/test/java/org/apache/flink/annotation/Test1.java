package org.apache.flink.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Description:
 * 注解的应用。
 * 给注解的属性赋值，赋值的方式是在注解的括号内以 value="" 形式，多个属性之前用 ，隔开。
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
//@TestAnnotation(id = 3, msg = "Hello Annotation")
//@TestAnnotation(value="defaultValue")
@TestAnnotation("defaultValue")
public class Test1 {

	@Check("Hello")
	int a;

	@Perform
	public void testMethod() {
	}


	public static void main(String[] args) {
		// 注解通过反射获取，通过 Class 对象的 isAnnotationPresent() 方法判断它是否应用了某个注解
		boolean hasAnnotation = Test1.class.isAnnotationPresent(TestAnnotation.class);
		if (hasAnnotation) {
			// 通过 getAnnotation() 方法来获取 Annotation 对象
			TestAnnotation testAnnotation = Test1.class.getAnnotation(TestAnnotation.class);
			System.out.println("id:" + testAnnotation.id());
			System.out.println("msg:" + testAnnotation.msg());
		}

		Annotation[] annotations = Test1.class.getAnnotations();
		for (Annotation annotation : annotations) {
			System.out.println(annotation);
		}

		try {
			Field a = Test1.class.getDeclaredField("a");
			a.setAccessible(true);
			// 获取一个成员变量上的注解
			Check check = a.getAnnotation(Check.class);

			if (check != null) {
				System.out.println("check value:" + check.value());
			}

			Method testMethod = Test1.class.getDeclaredMethod("testMethod");

			if (testMethod != null) {
				// 获取方法中的注解
				Annotation[] ans = testMethod.getAnnotations();
				for (int i = 0; i < ans.length; i++) {
					System.out.println("method testMethod annotation:" + ans[i].annotationType().getSimpleName());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}

