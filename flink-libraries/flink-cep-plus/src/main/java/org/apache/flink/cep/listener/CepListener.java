package org.apache.flink.cep.listener;

import org.apache.flink.cep.pattern.Pattern;

import java.io.Serializable;

public interface CepListener<T> extends Serializable {

	/**
	 * @param element 消费到的流数据
	 * @return 是否需要更新 cep 规则
	 */
	Boolean needChange(T element);

	/**
	 * 当 needChange 为 true 时会被调用，生成一个新的 pattern
	 *
	 * @param flagElement cep 规则逻辑注入数据
	 * @return 更新后的 cep Pattern
	 */
	Pattern<T, ?> returnPattern(T flagElement);
}
