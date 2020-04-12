package org.apache.flink.annotation;

import java.lang.annotation.*;

/**
 * Description:
 * 注解是一系列元数据，它提供数据用来解释程序代码，但是注解并非是所解释的代码本身的一部分。注解对于代码的运行效果没有直接影响。
 * 注解通过 @interface 关键字定义。
 * 注解的作用：
 * - 提供信息给编译器：编译器可以利用注解来探测错误和警告信息
 * - 编译阶段时的处理：软件工具可以利用注解信息来生成代码，HTML文档或做其他相应处理
 * - 运行时的处理：某些注解可以在程序运行时接受代码的提取
 *
 *
 * 定义了 id 和 msg 两个属性，使用的时候给其赋值。
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-10
 */
// 注解要想在运行时被成功提取，那么 @Retention(RetentionPolicy.RUNTIME) 是必须的
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TestAnnotation {

	// 注解只有成员变量，没有方法
	// 注解的成员变量在注解的定义中以"无形参的方法"形式来声明，其方法名定义了该成员变量的名字，返回值定义了该成员变量的类型
	// 类型必须是 8 中基本数据类型，外加类、接口、注解及它们的数组
	// 使用 default 关键字指定默认值
	int id() default -1;

	String msg() default "Hello";

	String value() default "";
}
