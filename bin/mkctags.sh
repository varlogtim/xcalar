#!/bin/bash

TMP1=`mktemp /tmp/mkctags.$$.XXXXXX`
TMP2=`mktemp /tmp/mkctags.$$.XXXXXX`
TMP3=`mktemp /tmp/mkctags.$$.XXXXXX`
rm -f $TMP1 $TMP2 $TMP3

if [ "$1" = "-c" ]
then
    echo "Building cscope database..."
    cscope -b -k -R -q
    echo "Done."
    exit 0
fi

find . -name *.[ch] > $TMP1
CURDIR=`pwd`

for filn in `cat $TMP1`
do
    echo $filn | sed "s@^\./@${CURDIR}/@" >> $TMP2
done

for dirn in `cat $TMP2`
do
    dirname $dirn >> $TMP3
done

cat $TMP3 | sort | uniq > $TMP1

echo "Making vi tags..."
for dirn in `cat $TMP1`
do
    cd $dirn
    ctags -L $TMP2
done

echo "Making emacs tags..."
for dirn in `cat $TMP1`
do
    cd $dirn
    ctags -e -L $TMP2
done

cd $CURDIR

echo "Building cscope database..."
cscope -b -k -R -q

rm -f $TMP1 $TMP2 $TMP3

echo "Done."
