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
import org.junit.Before;
import org.junit.BeforeClass;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;

public class BaseFDBTest {

    protected static Database DB;
    protected Subspace subspace;

    @BeforeClass
    public static void setupFDB() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();
    }

    @Before
    public void setup() throws Exception {
        subspace = new Subspace(new byte[] { 1, 2, 3 });
        cleanup();
    }

    @After
    public void cleanup() throws Exception {
        DB.run(txn -> {
            txn.clear(subspace.range());
            return null;
        });
    }

}
