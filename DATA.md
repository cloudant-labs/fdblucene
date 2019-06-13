Key/Value structure for Lucene indexes
======================================

Notes
-----

* All data is within a user-provided Subspace, which is excluded
 from the descriptions below.
* All keys are tuples unless otherwise stated.
* All values are _not_ tuples unless otherwise stated.
* Short strings are used to describe related keys as an aid to
 readability.

doc id space
------------

("d", $id) -> EMPTY_VALUE -- represents an in-use doc id. id is in range [0, 2^31-1).

Index level data
----------------

("i", "nd") -> $int -- The number of documents in the index

Document level data
-------------------

("s", $docID, $fieldName) -> ($fieldType, $fieldValue) -- StoredField
fieldType := "b" | "d" | "f" | "i" | "l" | "s"

Field level data
----------------

("f", $fieldName, "dc")          -> $docCount : LeafReader:getDocCount(field) and Terms.getDocCount()
("f", $fieldName, "sdf")         -> $sumDocFreq : LeafReader:getSumDocFreq(field) and Terms.getSumDocFreq()
("f", $fieldName, "sttf")        -> $sumTotalTermFreq : LeafReader:getSumTotalTermFreq(field) and Terms.getSumTotalTermFreq()
("ndv", $fieldName, $docID)      -> $long : LeafReader.getNumericDocValues(field)
("nv", $fieldName, $docID)       -> $long : LeafReader.getNormValues(field)
BinaryDocValues -- TODO
PointValues -- TODO
SortedDocValues -- TODO
SortedNumericDocValues -- TODO
SortedSetDocValues -- TODO

Term level data
---------------

("t", $fieldName, $term, "df") -> $docFreq : LeafReader.docFreq(term), TermsEnum.docFreq(), TermsEnum.totalTermFreq
("t", $fieldName, $term, "ttf") -> $totalTermFreq :  TermsEnum.totalTermFreq

Postings
--------

("p", $fieldName, $term, $docID) -> (freq)
("p", $fieldName, $term, $docID, $pos) -> (startOffset, endOffset, payload)
