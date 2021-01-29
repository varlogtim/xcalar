## Ubuntu Xenial build

For some reason on ub16 we need to disable the invalid-offsetof warning. We're using
the exact same compiler (optclang5).

Build the `ub16-build` container

    make -C $XLRDIR/docker/ub16

Run the build container

    cd $XLRDIR
    cshell ub16
    cmBuild clean
    CXX_FLAGS="-Wno-invalid-offsetof" cmBuild config prod
    cmBuild



