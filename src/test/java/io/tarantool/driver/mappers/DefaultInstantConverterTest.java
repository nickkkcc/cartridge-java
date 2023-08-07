package io.tarantool.driver.mappers;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;

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
    @Test
    void toValue() throws IOException {
        DefaultInstantToExtensionValueConverter converter = new DefaultInstantToExtensionValueConverter();
        MessagePacker packer = MessagePack.newDefaultBufferPacker();
        Base64.Encoder encoder = Base64.getEncoder();
        Instant instant = LocalDateTime.parse("2022-10-25T12:03:58").toInstant(ZoneOffset.UTC);
        byte[] result = ((MessageBufferPacker) packer.packValue(converter.toValue(instant))).toByteArray();
        assertEquals("2ASu0FdjAAAAAAAAAAAAAAAA", encoder.encodeToString(result));
    }

    @Test
    void fromValue() throws IOException {
        DefaultExtensionValueToInstantConverter converter = new DefaultExtensionValueToInstantConverter();
        Base64.Decoder base64decoder = Base64.getDecoder();
        Instant instant = LocalDateTime.parse("2022-10-25T12:03:58").toInstant(ZoneOffset.UTC);
        byte[] packed = base64decoder.decode("2ASu0FdjAAAAAAAAAAAAAAAA");
        ExtensionValue value = MessagePack.newDefaultUnpacker(packed).unpackValue().asExtensionValue();
        assertEquals(instant, converter.fromValue(value));
    }

    @Test
    void canConvertValue() {
        DefaultExtensionValueToInstantConverter converter = new DefaultExtensionValueToInstantConverter();
        assertFalse(converter.canConvertValue(ValueFactory.newExtension((byte) 100, new byte[]{0})));
        assertFalse(converter.canConvertValue(ValueFactory.newExtension((byte) 0x01, new byte[]{0})));
        ImmutableExtensionValue value = ValueFactory.newExtension((byte) 0x04, new byte[]{0});
        assertTrue(converter.canConvertValue(value));
    }

    @Test
    void testCheckMaxDatetimeValue() {
        Instant invalidTime = LocalDateTime.of(5879611,7,11,0,0,1,0)
            .toInstant(ZoneOffset.UTC);
        Instant invalidMaxTime = Instant.MAX;
        Instant normalTime = LocalDateTime.of(5879611,7,11,0,0,0,999999999)
            .toInstant(ZoneOffset.UTC);
        Instant nowTime = Instant.now();
        DefaultInstantToExtensionValueConverter converter = new DefaultInstantToExtensionValueConverter();
        assertAll(
            () -> assertThrows(MessagePackValueMapperException.class, () -> converter.toValue(invalidTime)),
            () -> assertThrows(MessagePackValueMapperException.class, () -> converter.toValue(invalidMaxTime)),
            () -> assertDoesNotThrow(() -> converter.toValue(normalTime)),
            () -> assertDoesNotThrow(() -> converter.toValue(nowTime))
            );
    }

    @Test
    void testCheckMinDateTimeValue() {
        Instant invalidTime = LocalDateTime.of(-5879610,6,21,23,59,59,999999999)
            .toInstant(ZoneOffset.UTC);
        Instant invalidMinTime = Instant.MIN;
        Instant normalTime = LocalDateTime.of(-5879610,6,22,0,0,0,0)
            .toInstant(ZoneOffset.UTC);
        Instant nowTime = Instant.now();
        DefaultInstantToExtensionValueConverter converter = new DefaultInstantToExtensionValueConverter();
        assertAll(
            () -> assertThrows(MessagePackValueMapperException.class, () -> converter.toValue(invalidTime)),
            () -> assertThrows(MessagePackValueMapperException.class, () -> converter.toValue(invalidMinTime)),
            () -> assertDoesNotThrow(() -> converter.toValue(normalTime)),
            () -> assertDoesNotThrow(() -> converter.toValue(nowTime))
        );
    }
}
