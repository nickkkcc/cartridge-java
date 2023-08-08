package io.tarantool.driver.api.eval;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.integration.SingleInstanceExampleIT;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class EvalTest {

    private static final Logger log = LoggerFactory.getLogger(SingleInstanceExampleIT.class);
    private static final TarantoolContainer tt = new TarantoolContainer().
        withScriptFileName("single-instance.lua")
        .withLogConsumer(new Slf4jLogConsumer(log));

    @BeforeAll
    static void beforeAll() {
        tt.start();
    }

    @AfterAll
    static void afterALl() {
        tt.stop();
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setup() {
        return TarantoolClientFactory.createClient()
            .withCredentials(tt.getUsername(), tt.getPassword())
            .withAddress(tt.getHost(), tt.getPort()).build();
    }

    // Fix for https://github.com/tarantool/cartridge-java/issues/408
    @Test
    void testEvalWithNullNanosInstant() throws Exception {
        try (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = setup()) {
            Collection<Instant> times = Arrays.asList(
                Instant.now(),
                Instant.ofEpochSecond(23456, 0),
                Instant.EPOCH
            );
            times.forEach(instant -> {
                try {
                    assertEquals(instant, client.eval("return ...", Collections.singletonList(instant))
                        .get().get(0));
                } catch (InterruptedException | ExecutionException e) {
                    throw new TarantoolClientException(e);
                }
            });
        }
    }
}
