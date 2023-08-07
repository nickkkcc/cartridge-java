package io.tarantool.driver.mappers.converters.object;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import io.tarantool.driver.mappers.MessagePackValueMapperException;
import io.tarantool.driver.mappers.converters.ObjectConverter;

/**
 * Default {@link java.time.Instant} to {@link ExtensionValue} converter
 *
 * @author Anastasiia Romanova
 * @author Artyom Dubinin
 */
public class DefaultInstantToExtensionValueConverter implements ObjectConverter<Instant, ExtensionValue> {

    private static final long serialVersionUID = 20221025L;
    // See https://github.com/tarantool/tarantool/blob/b2a001cc0f46fd9c53e576a74fa6263c6e6069bf/src/lua/datetime.lua
    private static final long MAX_EPOCH_SECOND = 185480451417600L;
    private static final long MIN_EPOCH_SECOND = -185604722870400L;

    private static final byte DATETIME_TYPE = 0x04;


    private byte[] toBytes(Instant value) throws IOException {
        if (value.getEpochSecond() > MAX_EPOCH_SECOND) {
            throw new IOException(
                String.format("Dates greater than %s are not supported",
                    Instant.ofEpochSecond(MAX_EPOCH_SECOND, 999999999)));
        } else if (value.getEpochSecond() < MIN_EPOCH_SECOND) {
            throw new IOException(
                String.format("Dates lesser than %s are not supported",
                    Instant.ofEpochSecond(MIN_EPOCH_SECOND, 0)));
        }
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value.getEpochSecond());
        buffer.putInt(value.getNano());
        return buffer.array();
    }

    @Override
    public ExtensionValue toValue(Instant object) {
        try {
            return ValueFactory.newExtension(DATETIME_TYPE, toBytes(object));
        } catch (IOException e) {
            throw new MessagePackValueMapperException(
                String.format("Failed to pack Instant %s to MessagePack entity", object), e);
        }
    }
}
