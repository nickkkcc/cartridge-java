package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Base class for all operation options that may have a configurable timeout.
 *
 * @author Alexey Kuzin
 */
public interface OperationWithTimeoutOptions<T extends OperationWithTimeoutOptions<T>> extends Options, Self<T> {

    String TIMEOUT = "timeout";

    /**
     * Specifies timeout for waiting for a server response for the operation.
     * Configured request timeout for that client will be used by default.
     *
     * @param timeout request timeout, in milliseconds
     * @return this options instance
     */
    default T withTimeout(int timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout should be greater than 0");
        }
        addOption(TIMEOUT, timeout);
        return self();
    }

    /**
     * Return operation timeout.
     *
     * @return timeout, in milliseconds.
     */
    default Optional<Integer> getTimeout() {
        return getOption(TIMEOUT, Integer.class);
    }
}
