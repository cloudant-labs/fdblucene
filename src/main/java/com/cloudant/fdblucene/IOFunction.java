package com.cloudant.fdblucene;

import java.io.IOException;

/**
 * A Function that can throw IOException.
 */
@FunctionalInterface
interface IOFunction<T, R> {
    R apply(T t) throws IOException;
}
