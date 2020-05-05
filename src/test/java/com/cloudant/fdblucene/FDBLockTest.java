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

import org.apache.lucene.store.BaseLockFactoryTestCase;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.apple.foundationdb.Database;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FDBLockTest extends BaseLockFactoryTestCase {

    private static Database DB;

    @BeforeClass
    public static void setup() throws Exception {
        DB = FDBUtil.getTestDb(true);
    }

    @AfterClass
    public static void cleanup() {
        FDBUtil.clear(DB);
        DB.close();
    }

    @Override
    protected Directory getDirectory(final Path path) throws IOException {
        return FDBDirectory.open(DB, path, null);
    }

}
