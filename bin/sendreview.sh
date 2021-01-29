#!/bin/bash

PROTOTYPEREPO=prototype
SRCBRANCH=$PROTOTYPEREPO/trunk
NEWCOMMIT=HEAD

TMPDIR=`mktemp /tmp/sendreview.XXXXXX`
rm -rf $TMPDIR
mkdir $TMPDIR

usage()
{
    echo "Usage:" 1>&2
    echo "        sendreview.sh  -r <reviewName> -u <userList> [-b <srcBranch>] [-s <srcCommit>] [-n <newCommit>]" 1>&2
    echo "" 1>&2
    echo "        When not specified, srcBranch defaults to $SRCBRANCH" 1>&2
    echo "        When not specified, newCommit defaults to $NEWCOMMIT" 1>&2
    echo "        When not specified, srcCommit defaults to the first common ancestor of branch and newCommit" 1>&2
    rm -rf $TMPDIR
    exit 1
}

getCommonAncestor()
{
    b1=$1
    b2=$2

    export ANCESTORCOMMIT=""
    for commit in `git log -1000 --oneline $b1 | cut -f1 -d' '`
    do
        git log -1000 --oneline $b2 | cut -f1 -d' ' | grep -q $commit
        if [ "$?" = "0" ]
        then
            ANCESTORCOMMIT=$commit
            return 0
            break
        fi
    done

    return 1
}

SRCCOMMIT=""
REVNAME=""
USERLIST=""

while getopts "b:s:n:r:u:" opt; do
  case $opt in
      b) SRCBRANCH=$OPTARG;;
      s) SRCCOMMIT=$OPTARG;;
      n) NEWCOMMIT=$OPTARG;;
      r) REVNAME=$OPTARG;;
      u) USERLIST=$OPTARG;;
      *) usage;;
  esac
done

if [ "$REVNAME" = "" ]
then
    echo "-r <reviewName> is a required argument" 1>&2
    usage
fi
if [ "$USERLIST" = "" ]
then
    echo "-u <userList> is a required argument" 1>&2
    usage
fi

SRCBRANCHSUFFIX=`echo $SRCBRANCH | sed s"@$PROTOTYPEREPO/@@"`
FULLREVPREFIX=user/$LOGNAME-$SRCBRANCHSUFFIX-$REVNAME
FULLREVNAME=$PROTOTYPEREPO/$FULLREVPREFIX

if [ "$SRCCOMMIT" = "" ]
then
    getCommonAncestor $SRCBRANCH $NEWCOMMIT
    SRCCOMMIT=$ANCESTORCOMMIT
fi

if [ "$SRCCOMMIT" = "" ]
then
    echo "Failed to find common ancestor between $SRCBRANCH and $NEWCOMMIT" 1>&2
    rm -rf $TMPDIR
    exit 1
fi

INCCOMMIT=""
# check for whether reviewName exists yet
git log $FULLREVNAME >/dev/null 2>&1
if [ "$?" = "0" ]
then
    # incremental case
    getCommonAncestor $FULLREVNAME $NEWCOMMIT
    INCCOMMIT=$ANCESTORCOMMIT
    if [ "$INCCOMMIT" = "" ]
    then
        echo "Failed to find incremental commit between $FULLREVNAME and $NEWCOMMIT" 1>&2
        echo "sendreview.sh does not yet support generating incremental reviews on a rebased newCommit" 1>&2
        rm -rf $TMPDIR
        exit 1
    fi
fi

git push $PROTOTYPEREPO $NEWCOMMIT:refs/heads/$FULLREVPREFIX
if [ "$?" != "0" ]
then
    echo "Failed to push $PROTOTYPEREPO $NEWCOMMIT:$FULLREVPREFIX" 1>&2
    rm -rf $TMPDIR
    exit 1
fi

NEWCOMMITSHA1=`git rev-parse $NEWCOMMIT | cut -c1-8`

SUBJECT="Review[$SRCBRANCHSUFFIX]: `git log -1 --oneline $NEWCOMMIT | cut -f2- -d' '`"

echo "Reviewers: $USERLIST, anyone curious" > $TMPDIR/body
echo "" >> $TMPDIR/body
git log -1 $NEWCOMMIT | tail -n +4 >> $TMPDIR/body
printf "\n$ git fetch\n\n" >> $TMPDIR/body
if [ "$INCCOMMIT" != "" ]
then
    echo "Incremental:" >> $TMPDIR/body
    echo "  $ git difftool -a -y $INCCOMMIT $NEWCOMMITSHA1" >> $TMPDIR/body
fi
echo "Full:" >> $TMPDIR/body
echo "  $ git difftool -a -y $SRCCOMMIT $NEWCOMMITSHA1" >> $TMPDIR/body
printf "\nSent via sendreview.sh $*\n" >> $TMPDIR/body

cat $TMPDIR/body | mail -s "$SUBJECT" sys-eng@xcalar.com

rm -rf $TMPDIR

exit 0
