package com.cloudant.fdblucene;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.junit.BeforeClass;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class FDBDirectoryTest extends BaseDirectoryTestCase {

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
