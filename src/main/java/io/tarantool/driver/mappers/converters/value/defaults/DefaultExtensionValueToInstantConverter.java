package io.tarantool.driver.mappers.converters.value.defaults;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

import org.msgpack.value.ExtensionValue;

import io.tarantool.driver.mappers.MessagePackValueMapperException;
import io.tarantool.driver.mappers.converters.ValueConverter;

/**
 * Default {@link ExtensionValue} to {@link java.time.Instant} converter
 *
 * @author Anastasiia Romanova
 * @author Artyom Dubinin
 */
public class DefaultExtensionValueToInstantConverter implements ValueConverter<ExtensionValue, Instant> {

    private static final long serialVersionUID = 20221025L;

    // See https://github.com/tarantool/tarantool/blob/b2a001cc0f46fd9c53e576a74fa6263c6e6069bf/src/lua/datetime.lua
    private static final long MAX_EPOCH_SECOND = 185480451417600L;
    private static final long MIN_EPOCH_SECOND = -185604722870400L;

    private static final byte DATETIME_TYPE = 0x04;

    private Instant fromBytes(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.put(bytes).rewind();
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long epochSeconds = buffer.getLong();
        int nanos = buffer.getInt();

        if (epochSeconds > MAX_EPOCH_SECOND) {
            throw new IOException(
                String.format("Dates greater than %s are not supported",
                    Instant.ofEpochSecond(MAX_EPOCH_SECOND, 999999999)));
        } else if (epochSeconds < MIN_EPOCH_SECOND) {
            throw new IOException(
                String.format("Dates lesser than %s are not supported",
                    Instant.ofEpochSecond(MIN_EPOCH_SECOND, 0)));
        }
        return Instant.ofEpochSecond(epochSeconds).plusNanos(nanos);
    }

    @Override
    public Instant fromValue(ExtensionValue value) {
        try {
            return fromBytes(value.getData());
        } catch (IOException e) {
            throw new MessagePackValueMapperException(
                String.format("Failed to unpack Instant from MessagePack entity %s", value), e);
        }
    }

    @Override
    public boolean canConvertValue(ExtensionValue value) {
        return value.getType() == DATETIME_TYPE;
    }
}
