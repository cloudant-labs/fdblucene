package com.cloudant.fdblucene;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.BaseLockFactoryTestCase;
import org.apache.lucene.store.Directory;
import org.junit.BeforeClass;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FDBLockTest extends BaseLockFactoryTestCase {

    private static Database DB;

    @BeforeClass
    public static void setup() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();
    }

    @Override
    protected Directory getDirectory(final Path path) throws IOException {
        return FDBDirectory.open(DB, path);
    }

}
