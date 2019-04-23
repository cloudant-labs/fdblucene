# FDBLucene

FDBLucene is a new project to store Lucene indexes into FoundationDB
while providing high performance for both indexing and searching.

An introduction to Lucene is necessary to ground the decisions
made in FDBLucene's design:

Lucene expects to write to disk (via a file system) and uses an
inverted index for this reason. To balance the optimal on-disk format
with the need to efficiently update an index, Lucene creates multiple
"segments". Each of these segments is an index in its own right,
though Lucene makes it easy to search across all segments.

Because Lucene assumes a file system, it defines its own transactional
semantics. Firstly, a lock file is used to ensure there is only a
single writer to the index at a time. Secondly, data that is written
to a file is not required to be visible until the file is
closed. Finally, there is a central file (called the segments file)
which names the other files in the directory which constitute the
index. This allows Lucene to build files in the index without making
them immediately visible. The segments file is itself updated
atomically.

These design decisions with Lucene guide us to where, and whether, to
apply FDB transactional semantics. When writing to a new file, for
example, we have no need to put a transaction around the data we're
writing. FoundationDB, of course, requires one, but it has no semantic
meaning to Lucene. We can therefore buffer as much data as we like to
form an optimal transaction size. In contrast, the `rename` method
is atomic.

Lucene creates empty files, fills them with data by appending, and
then closes them. The files are never updated again. They are
therefore highly cacheable. FDBLucene exploits this property by
caching every `page` that it reads from any file. The behaviour,
and capacity, of that cache is configurable by the user as FDBLucene
uses Apache JCS (http://commons.apache.org/jcs/). The cache for an
individual file is only valid while it is open in order to avoid any
cache coherency issues if an index is deleted and recreated.
