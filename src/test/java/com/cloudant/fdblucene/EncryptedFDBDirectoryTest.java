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

import java.io.IOException;
import java.nio.file.Path;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.BeforeClass;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class EncryptedFDBDirectoryTest extends BaseDirectoryTestCase {

    private static Database DB;

    @BeforeClass
    public static void setup() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();

    }

    @Before
    public void clear() {
        DB.run(txn -> {
            txn.clear(new byte[0], new byte[] { (byte) 254, (byte) 255, (byte) 255 });
            return null;
        });
    }

    @Override
    protected Directory getDirectory(final Path path) throws IOException {
        final SecretKey secretKey = new SecretKeySpec(new byte[32], "AES");
        return FDBDirectory.open(DB, path, secretKey);
    }

}
