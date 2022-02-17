package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.retryNetworkErrors;
import static io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.retryTarantoolNoSuchProcedureErrors;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ReconnectIT extends SharedCartridgeContainer {

    private static final Logger logger = LoggerFactory.getLogger(ReconnectIT.class);

    private static String USER_NAME;
    private static String PASSWORD;

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
            new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

    @BeforeAll
    public static void setUp() throws TimeoutException {
        startCluster();

        WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer);
        waitingConsumer.waitUntil(f -> f.getUtf8String().contains("The cluster is balanced ok"));

        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    /**
     * Checking if this test is valid is here
     * {@link TarantoolErrorsIT#test_should_throwTarantoolNoSuchProcedureException_ifProcedureIsNil}
     */
    @Test
    public void test_should_reconnect_ifCrudProcedureIsNotDefined() {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clusterClient = getClusterClient();
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> retryingClient = getRetryingTarantoolClient();

        try {
            //save procedure to tmp variable set it to nil and call
            clusterClient.eval("rawset(_G, 'tmp_test_no_such_procedure', test_no_such_procedure)")
                    .thenAccept(c -> clusterClient.eval("rawset(_G, 'test_no_such_procedure', nil)"))
                    .thenApply(c -> clusterClient.call("test_no_such_procedure"))
                    .join();
        } catch (CompletionException exception) {
            assertTrue(exception.getCause() instanceof TarantoolNoSuchProcedureException);
        }

        //start another thread that will return the procedure back after 100 ms
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            clusterClient.eval("rawset(_G, 'test_no_such_procedure', tmp_test_no_such_procedure)").join();
        }).start();

        assertDoesNotThrow(() -> retryingClient.call("test_no_such_procedure").join());
        assertEquals("test_no_such_procedure", retryingClient.call("test_no_such_procedure").join().get(0));
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getClusterClient() {
        return TarantoolClientFactory
                .createClient()
                .withAddresses(
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3311)),
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3312))
                )
                .withCredentials(USER_NAME, PASSWORD)
                .withConnections(10)
                .build();
    }

    @Test
    public void test_should_reconnect_ifReconnectIsInvoked() throws Exception {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = getRetryingTarantoolClient();

        // getting all routers uuids
        final Set<String> routerUuids = getInstancesUuids(client);

        // stop routers
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "router");
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "second-router");

        // check that there is only one instance left
        assertEquals(getInstanceUuid(client), getInstanceUuid(client));

        // start routers
        container.execInContainer("cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d");

        client.refresh();
        Thread.sleep(3000);

        // getting all routers uuids after restarting
        final Set<String> uuidsAfterReconnect = getInstancesUuids(client);

        // check that amount of routers is equal initial amount
        assertEquals(routerUuids.size(), uuidsAfterReconnect.size());
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getRetryingTarantoolClient() {
        return TarantoolClientFactory.createClient()
                .withAddresses(
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3311)),
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3312))
                )
                .withCredentials(USER_NAME, PASSWORD)
                .withConnections(10)
                .withProxyMethodMapping()
                .withRetryingByNumberOfAttempts(5,
                        retryNetworkErrors().or(retryTarantoolNoSuchProcedureErrors()),
                        factory -> factory.withDelay(300)
                )
                .build();
    }

    @Test
    void should_removeUnavailableHostsFromAddressProvider_ifDiscoveryProcedureReturnStatusNotHealthyAndNotAvailable()
            throws Exception {
        initRouterStatuses();
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = initClientWithDiscovery();

        final Set<String> instancesUuids = getInstancesUuids(client);
        assertEquals(3, instancesUuids.size());

        replaceInstancesInfo(client, 1, "unavailable", 3301);
        replaceInstancesInfo(client, 2, "unavailable", 3311);
        Thread.sleep(1000);

        final Set<String> afterRoutersDisablingInstancesUuids = getInstancesUuids(client);
        assertEquals(1, afterRoutersDisablingInstancesUuids.size());

        replaceInstancesInfo(client, 1, "available", 3301);
        replaceInstancesInfo(client, 2, "available", 3311);
        Thread.sleep(1000);

        final Set<String> afterRoutersEnablingInstancesUuids = getInstancesUuids(client);
        assertEquals(3, afterRoutersEnablingInstancesUuids.size());
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> initClientWithDiscovery() {
        final BinaryClusterDiscoveryEndpoint discoveryEndpoint = BinaryClusterDiscoveryEndpoint.builder()
                .withEntryFunction("get_routers_status")
                .withEndpointProvider(() -> Arrays.asList(getTarantoolServerAddresses()))
                .withClientConfig(TarantoolClientConfig.builder()
                        .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                        .build()
                )
                .build();

        final BinaryDiscoveryClusterAddressProvider binaryDiscoveryClusterAddressProvider =
                new BinaryDiscoveryClusterAddressProvider(TarantoolClusterDiscoveryConfig.builder()
                        .withEndpoint(discoveryEndpoint)
                        .withDelay(50)
                        .build());

        return TarantoolClientFactory.createClient()
                .withAddressProvider(
                        new TestWrappedClusterAddressProvider(binaryDiscoveryClusterAddressProvider, container))
                .withCredentials(USER_NAME, PASSWORD)
                .withConnections(10)
                .withProxyMethodMapping()
                .build();
    }

    private void initRouterStatuses() {
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> initClient =
                TarantoolClientFactory.createClient()
                        .withAddresses(getTarantoolServerAddresses())
                        .withCredentials(USER_NAME, PASSWORD)
                        .withConnections(1)
                        .withProxyMethodMapping()
                        .build();
        initClient.call("init_router_status").join();
    }

    private void replaceInstancesInfo(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client,
                                      int id, String status, Integer port) {
        final TarantoolTuple tuple = tupleFactory
                .create(id, 1, UUID.randomUUID().toString(), status, String.format("%s:%s", "localhost", port));

        client.space("instances_info").replace(tuple).join();
    }

    @NotNull
    private TarantoolServerAddress[] getTarantoolServerAddresses() {
        return new TarantoolServerAddress[]{
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3311)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3312))
        };
    }

    /**
     * Return all instances uuids from cluster, using round robin connection selection strategy
     *
     * @param client Tarantool client
     * @return set of instances uuids from cluster
     */
    private Set<String> getInstancesUuids(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        String firstUuid = getInstanceUuid(client);

        final Set<String> instancesUuids = new HashSet<>();
        instancesUuids.add(firstUuid);

        String currentUuid = "";
        while (!firstUuid.equals(currentUuid)) {
            currentUuid = getInstanceUuid(client);
            instancesUuids.add(currentUuid);
        }

        return instancesUuids;
    }

    private String getInstanceUuid(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return (String) client.eval("return box.info().uuid").join().get(0);
    }
}
