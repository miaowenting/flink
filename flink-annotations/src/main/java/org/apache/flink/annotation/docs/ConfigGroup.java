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

package org.apache.flink.annotation.docs;

import org.apache.flink.annotation.Internal;

import java.lang.annotation.Target;

/**
 * A class that specifies a group of config options. The name of the group will be used as the basis for the
 * filename of the generated html file, as defined in {@link ConfigOptionsDocGenerator}.
 *
 * @see ConfigGroups
 *
 * 指定一组配置选项，组的名称将用作生成 HTML 文件名，keyPrefix 用于匹配配置项名称前缀。
 * 如 @ConfigGroup(name = "firstGroup", keyPrefix = "first")，生成的 HTML 文件名为 firstGroup ，其中的配置项名称都是以 first 开头的。
 */
@Target({})
@Internal
public @interface ConfigGroup {
	String name();
	String keyPrefix();
}
