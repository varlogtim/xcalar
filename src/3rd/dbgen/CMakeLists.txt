add_executable(dbgen build.c driver.c bm_utils.c rnd.c print.c load_stub.c bcd2.c speed_seed.c text.c permute.c rng64.c)
target_compile_options(dbgen PUBLIC -Wno-strict-prototypes -Wno-error -g -DDBNAME=\"dss\" -DLINUX -DORACLE -DTPCH)
add_dependencies(qa dbgen)
