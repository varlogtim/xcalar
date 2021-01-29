add_definitions("-Wno-missing-field-initializers")
xc_add_library(liblmdb STATIC mdb.c midl.c)
