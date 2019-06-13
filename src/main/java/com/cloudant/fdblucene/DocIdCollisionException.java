package com.cloudant.fdblucene;

final class DocIdCollisionException extends RuntimeException {

    private static final long serialVersionUID = -8418485954882085985L;

    DocIdCollisionException(final int docID) {
        super("collision on doc id " + docID);
    }

}
