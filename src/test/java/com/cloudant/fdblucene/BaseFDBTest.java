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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;

public class BaseFDBTest {

    protected static Database DB;
    protected Subspace subspace = new Subspace(new byte[] { 1, 2, 3 });

    @BeforeClass
    public static void setup() throws Exception {
        DB = FDBUtil.getTestDb(true);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        FDBUtil.clear(DB);
        DB.close();
    }

    @Before
    @After
    public void clearSubspace() {
        DB.run(txn -> {
            txn.clear(subspace.range());
            return null;
        });
    }

}
