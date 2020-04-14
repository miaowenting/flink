/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * This annotations declares that a function, field, constructor, or entire type, is only visible for
 * testing purposes.
 *
 * <p>This annotation is typically attached when for example a method should be {@code private}
 * (because it is not intended to be called externally), but cannot be declared private, because
 * some tests need to have access to it.
 *
 * 标注有些方法、属性、构造函数、类等在 test 时可见，用于测试。
 * 例如，当方法是 private 的，不打算在外部去调用的，但是有些内部测试需要访问它，所以加上 VisibleForTesting 注解进行内部测试。
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR })
@Internal
public @interface VisibleForTesting {}
