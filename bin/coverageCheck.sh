#!/bin/bash

failhdr()
{
    if [ ! -e "$FAILMRK" ]
    then
        echo "============================================================" 1>&2
	echo "*** CODE COVERAGE CHECK FAILED" 1>&2
    fi
}

rungcov()
{
    if [ "$1" != "" ]
    then
	specificFile=`basename $1`
    else
	specificFile=""
    fi
    rm -f $SPECMRK
    gcov *.gcda > $GCOVOUTPUT 2>/dev/null
    cat $GCOVOUTPUT | while read -r line1
    do
	read -r line2
	read -r line3
	read -r line4

	filn=`echo $line1 | cut -f2 -d\'`

	# deal with random gcov output
	if [ "$filn" = "" ]
	then
	    echo "*** WARNING: Skipping input filn:$filn line1:$line1 line2:$line2 line3:$line3 line4:$line4" 1>&2
	    continue
	fi
	echo $filn | fgrep -q '.c'
	if [ "$?" != "0" ]
	then
	    echo "*** WARNING: Skipping input filn:$filn line1:$line1 line2:$line2 line3:$line3 line4:$line4" 1>&2
	    continue
	fi

	filn=`basename $filn`
	if [ "$specificFile" != "" ]
	then
	    if [ "$specificFile" != "$filn" ]
	    then
		continue
	    else
		echo $filn > $SPECMRK
	    fi
	fi

	grep -v $filn $CFILES > $CFILES2
	mv $CFILES2 $CFILES
	pct=`echo $line2 | cut -f2 -d: | cut -f1 -d% | cut -f1 -d.`
	if [ "$pct" = "" ]
	then
	    pct=0
	fi
	if [ "$pct" -lt "$MIN_COV_PCT" ]
	then
	    failhdr
	    echo "*** $filn only has $pct% coverage; at least $MIN_COV_PCT% required" 1>&2
	    touch $FAILMRK
	fi
    done
    if  [ "$specificFile" != "" ] &&  [ ! -e "$SPECMRK" ]
    then
	failhdr
	echo "*** $specificFile lacks coverage; at least $MIN_COV_PCT% required" 1>&2
	touch $FAILMRK
    fi
}

MIN_COV_PCT=60
CHECKROOT=`pwd`

FAILMRK=`mktemp /tmp/XXXXXXcoverage1`
GCOVOUTPUT=`mktemp /tmp/XXXXXXcoverage2`
CFILES=`mktemp /tmp/XXXXXXcoverage3`
CFILES2=`mktemp /tmp/XXXXXXcoverage4`
SPECMRK=`mktemp /tmp/XXXXXXcoverage5`
SYMLINKS=`mktemp /tmp/XXXXXXcoverage6`
rm -f $FAILMRK $GCOVOUTPUT $CFILES $CFILES2 $SPECMRK $SYMLINKS

find . -maxdepth 1 -name \*.c > $CFILES 2>/dev/null
cp $CFILES $SYMLINKS

# first determine if this is a lib
ISLIB=0
pwd | grep -q 'src/lib/lib'
if [ "$?" = "0" ]
then
    ISLIB=1
fi

if [ "$ISLIB" = "1" ]
then
    # for libs the coverage files end up in the .libs dir
    cd .libs
    for filn in `cat $CFILES`
    do
	ln -sf ../$filn $filn 2>/dev/null
    done
else
    # for binaries the coverage files end up in the test dir
    if [ -d test ]
    then
        cd test
        for filn in `cat $CFILES`
        do
	    ln -sf ../$filn $filn 2>/dev/null
        done
    fi
fi

rungcov

# cleanup the symlinks we created
for filn in `cat $SYMLINKS`
do
    if [ -L "$filn" ]
    then
	rm $filn
    fi
done

# if there's anything left in $CFILES then it has 0 coverage;
# may be because this was a main() .c file.  Try running again
# in the parent directory
cd ..
for filn in `cat $CFILES`
do
    rungcov $filn
done

retval=0
if [ -e "$FAILMRK" ]
then
    echo "============================================================" 1>&2
    retval=1
fi

rm -f $FAILMRK $GCOVOUTPUT $CFILES $CFILES2 $SPECMRK $SYMLINKS

exit $retval
