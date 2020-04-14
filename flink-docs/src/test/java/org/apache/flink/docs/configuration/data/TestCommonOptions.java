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
 */

package org.apache.flink.docs.configuration.data;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Collection of test {@link ConfigOptions ConfigOptions}.
 *
 * mock 了一个带 @Documentation.CommonOption 注解的配置类
 */
public class TestCommonOptions {

	/**
	 * COMMON_OPTION 使用了 @Documentation.CommonOption 注解，position 使用默认值为 Integer.MAX_VALUE
	 */
	@Documentation.CommonOption
	public static final ConfigOption<Integer> COMMON_OPTION = ConfigOptions
		.key("first.option.a")
		.defaultValue(2)
		.withDescription("This is the description for the common option.");

	public static final ConfigOption<String> GENERIC_OPTION = ConfigOptions
		.key("second.option.a")
		.noDefaultValue()
		.withDescription("This is the description for the generic option.");

	/**
	 * COMMON_POSITIONED_OPTION 也是用了 @Documentation.CommonOption 注解，position 值指定为2，这个配置项肯定排在 COMMON_OPTION 前面
	 */
	@Documentation.CommonOption(position = 2)
	public static final ConfigOption<Integer> COMMON_POSITIONED_OPTION = ConfigOptions
		.key("third.option.a")
		.defaultValue(3)
		.withDescription("This is the description for the positioned common option.");
}
