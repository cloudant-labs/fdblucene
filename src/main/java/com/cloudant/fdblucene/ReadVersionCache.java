/*******************************************************************************
 * Copyright 2019 IBM Corporation
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.cloudant.fdblucene;

import java.util.concurrent.TimeUnit;
import com.apple.foundationdb.Transaction;

final class ReadVersionCache implements Cloneable {

    private static final long MAX_AGE = TimeUnit.NANOSECONDS.convert(4, TimeUnit.SECONDS);

    private long readVersion = -1L;

    private long readVersionAt;

    public ReadVersionCache() {
    }

    public void setReadVersion(final Transaction txn) {
        final long now = System.nanoTime();
        if (readVersion == -1L) {
            readVersion = txn.getReadVersion().join();
            readVersionAt = now;
            return;
        }
        final long readVersionAge = now - readVersionAt;
        if (readVersionAge > MAX_AGE) {
            readVersion = txn.getReadVersion().join();
            readVersionAt = now;
        } else {
            txn.setReadVersion(readVersion);
        }
    }

    public ReadVersionCache clone() {
      try {
        return (ReadVersionCache) super.clone();
      } catch (final CloneNotSupportedException e) {
        throw new Error("cannot happen!");
      }
    }

}
