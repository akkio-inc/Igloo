# Igloo's copy of CRoaring

This is [CRoaring](https://github.com/RoaringBitmap/CRoaring) v4.7.2 (commit
2e8395f1dbf286d7944a7276195a7c40cbcbfd4a), dual licensed under the Apache
License 2.0 and the MIT License (see LICENSE). Igloo uses it under the Apache
License 2.0. Its roaring bitmaps back the `rb` DuckDB extension in
`extensions/rb`, which counts distinct integer IDs exactly in a fraction of the
memory of a hash table.

`roaring.c` and `roaring.h` are CRoaring's single-file amalgamation, unchanged,
as its `amalgamation.sh` writes them. To regenerate them for another version:

```
git clone --depth 1 --branch vX.Y.Z https://github.com/RoaringBitmap/CRoaring
bash CRoaring/amalgamation.sh third_party/croaring   # writes roaring.c, roaring.h, roaring.hh
```

then remove `roaring.hh`, the C++ wrapper the extension does not use, and the
demo files the script writes, and update the version and commit above.
