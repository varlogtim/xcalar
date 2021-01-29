add_definitions("-Wno-undef")
xc_add_library(libdecnumber STATIC decNumber.c decContext.c decimal64.c decimal128.c)
