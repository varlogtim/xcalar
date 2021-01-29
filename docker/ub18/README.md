## Ubuntu Bionic build

Build the `ub18-build` container

    make -C $XLRDIR/docker/ub18

Enter the build container

    cd $XLRDIR
    cshell ub18

Once inside the container, you can run your regular build tools:

    . bin/xcsetenv
    cmBuild clean
    cmBuild config prod
    cmBuild



