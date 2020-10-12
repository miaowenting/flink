/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.apache.flink.cep;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.listener.CepListener;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * Description:
 *
 * @author miaowenting
 * @version $Id: CepPlusITCase.java, v 0.1 2020-10-10 7:18 PM miaowenting Exp $$
 */
public class CepPlusITCase extends AbstractTestBase implements Serializable {

	/**
	 * 测试逻辑注入 CEP 规则
	 */
	@Test
	public void testDynamicLogicInjectCepRule() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "barfoo", 1.0),
			new Event(2, "start-1", 2.0),
			new Event(3, "foobar", 3.0),
			new SubEvent(4, "foo", 4.0, 1.0),
			new Event(5, "middle-1", 5.0),
			new SubEvent(6, "middle-1", 6.0, 2.0),
			new SubEvent(7, "bar", 3.0, 3.0),
			new Event(42, "42", 42.0),
			new Event(8, "end-1", 1.0),

			// 注入改变 cep 规则的逻辑事件，监听到这个事件则使用新的 cep 规则
			new Event(-1, "change", -1),

			new Event(9, "start-2", 1.0),
			new SubEvent(10, "middle-2", 1.0, 1.0),
			new Event(11, "end-2", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start-1");
			}
		})
			.followedByAny("middle").subtype(SubEvent.class).where(
				new SimpleCondition<SubEvent>() {

					@Override
					public boolean filter(SubEvent value) throws Exception {
						return value.getName().equals("middle-1");
					}
				}
			)
			.followedByAny("end").where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end-1");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern)
			.registerListener(new CepListener<Event>() {

				@Override
				public Boolean needChange(Event value) {
					return "change".equalsIgnoreCase(value.getName());
				}

				@Override
				public Pattern returnPattern(Event flagElement) {
					System.out.println("Receive dynamic logic injection cep rule -> " + flagElement.toString()
						+ ", Will use new cep rule.");

					return Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

						@Override
						public boolean filter(Event value) throws Exception {
							return value.getName().equals("start-2");
						}
					})
						.followedByAny("middle").subtype(SubEvent.class).where(
							new SimpleCondition<SubEvent>() {

								@Override
								public boolean filter(SubEvent value) throws Exception {
									return value.getName().equals("middle-2");
								}
							}
						)
						.followedByAny("end").where(new SimpleCondition<Event>() {

							@Override
							public boolean filter(Event value) throws Exception {
								return value.getName().equals("end-2");
							}
						});
				}
			}).flatSelect((p, o) -> {
				StringBuilder builder = new StringBuilder();

				builder.append(p.get("start").get(0).getId()).append(",")
					.append(p.get("middle").get(0).getId()).append(",")
					.append(p.get("end").get(0).getId());

				o.collect(builder.toString());
			}, Types.STRING);


		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		assertEquals(Arrays.asList("2,6,8", "9,10,11"), resultList);

	}

}
