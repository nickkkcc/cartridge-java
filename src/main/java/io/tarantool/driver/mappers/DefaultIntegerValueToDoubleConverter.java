package io.tarantool.driver.mappers;

import org.msgpack.value.IntegerValue;

/**
 * Default {@link IntegerValue} to {@code Double} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultIntegerValueToDoubleConverter implements ValueConverter<IntegerValue, Double> {

    @Override
    public Double fromValue(IntegerValue value) {
        return value.toDouble();
    }
}
