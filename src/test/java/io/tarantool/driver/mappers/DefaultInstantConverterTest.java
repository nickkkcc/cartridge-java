package io.tarantool.driver.mappers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ImmutableExtensionValue;
import org.msgpack.value.ValueFactory;

import io.tarantool.driver.mappers.converters.object.DefaultInstantToExtensionValueConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultExtensionValueToInstantConverter;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultInstantConverterTest {

    private static final Collection<Instant> invalidTime = Arrays.asList(
        LocalDateTime.of(5879611, 7, 11, 0, 0, 1, 0)
            .toInstant(ZoneOffset.UTC),
        Instant.MAX,
        LocalDateTime.of(-5879610, 6, 21, 23, 59, 59, 999999999)
            .toInstant(ZoneOffset.UTC),
        Instant.MIN


    );
    private static final Collection<Instant> normalTime = Arrays.asList(
        LocalDateTime.of(5879611, 7, 11, 0, 0, 0, 999999999)
            .toInstant(ZoneOffset.UTC),
        LocalDateTime.of(-5879610, 6, 22, 0, 0, 0, 0)
            .toInstant(ZoneOffset.UTC),
        Instant.now()
    );

    private static final DefaultInstantToExtensionValueConverter instantToExtensionValueConverter =
        new DefaultInstantToExtensionValueConverter();
    private static final DefaultExtensionValueToInstantConverter extensionValueToInstantConverter =
        new DefaultExtensionValueToInstantConverter();

    private ExtensionValue invalidToValue(Instant object) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(object.getEpochSecond());
        buffer.putInt(object.getNano());
        return ValueFactory.newExtension((byte) 4, buffer.array());
    }

    @Test
    void toValue() throws IOException {
        MessagePacker packer = MessagePack.newDefaultBufferPacker();
        Base64.Encoder encoder = Base64.getEncoder();
        Instant instant = LocalDateTime.parse("2022-10-25T12:03:58").toInstant(ZoneOffset.UTC);
        byte[] result = ((MessageBufferPacker) packer.packValue(instantToExtensionValueConverter.toValue(instant)))
            .toByteArray();
        assertEquals("2ASu0FdjAAAAAAAAAAAAAAAA", encoder.encodeToString(result));
    }

    @Test
    void fromValue() throws IOException {

        Base64.Decoder base64decoder = Base64.getDecoder();
        Instant instant = LocalDateTime.parse("2022-10-25T12:03:58").toInstant(ZoneOffset.UTC);
        byte[] packed = base64decoder.decode("2ASu0FdjAAAAAAAAAAAAAAAA");
        ExtensionValue value = MessagePack.newDefaultUnpacker(packed).unpackValue().asExtensionValue();
        assertEquals(instant, extensionValueToInstantConverter.fromValue(value));
    }

    @Test
    void canConvertValue() {
        assertFalse(extensionValueToInstantConverter.canConvertValue(ValueFactory
            .newExtension((byte) 100, new byte[]{0})));
        assertFalse(extensionValueToInstantConverter.canConvertValue(ValueFactory
            .newExtension((byte) 0x01, new byte[]{0})));
        ImmutableExtensionValue value = ValueFactory.newExtension((byte) 0x04, new byte[]{0});
        assertTrue(extensionValueToInstantConverter.canConvertValue(value));
    }

    @Test
    void testToValueWithMaxMinTime() {
        invalidTime.forEach(
            instant -> assertThrows(MessagePackValueMapperException.class, () -> instantToExtensionValueConverter
                .toValue(instant)));
        normalTime.forEach(instant -> assertDoesNotThrow(() -> instantToExtensionValueConverter.toValue(instant)));
    }

    @Test
    void testFromValueWithMaxMinTime() {

        invalidTime.forEach(instant -> assertThrows(MessagePackValueMapperException.class,
            () -> extensionValueToInstantConverter.fromValue(invalidToValue(instant))));

        normalTime.forEach(instant -> {
            ExtensionValue value = invalidToValue(instant);
            Instant[] time = new Instant[1];
            assertAll(
                () -> assertDoesNotThrow(() -> time[0] = extensionValueToInstantConverter.fromValue(value)),
                () -> assertEquals(instant, time[0])
            );
        });
    }
}
