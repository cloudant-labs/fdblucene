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
