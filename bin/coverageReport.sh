#!/bin/bash

usage()
{
    echo "Usage: coverageReport.sh --type <reportType> [--output <outputDir>] [--browser]" 1>&2
    echo "       --type Determines the type of coverage report to generate." 1>&2
    echo "              Can be \"html\"." 1>&2
    echo "       --output Directory to place output in. Uses current directory if unspecified." 1>&2
    echo "       --browser Launches a browser to view coverage report." 1>&2
    echo "                 Type must be html." 1>&2
}

fail()
{
    # Print error message and/or usage then exit.
    
    local message=$1
    local skipUsage=$2
    
    echo ""
    
    if [ "$message" != "" ]; then
        # Print $message in red.
        echo -e "\e[31m${message}\e[0m"
        echo ""
    fi

    if [ "$skipUsage" = false -o "$skipUsage" = "" ]; then
        usage
        echo ""
    fi
    
    exit 1
}

coverageReportHtml()
{
    # Generate code coverage report in html format using lcov and genhtml.
    
    local outputDir=$1
    local launchBrowser=$2
    
    # Clean previous report.
    rm -f "${outputDir}/coverageTotal.info" "${outputDir}/coverage.info"
    rm -rf "${outputDir}/coverage/"

    # Binaries must already have been instrumented to collect coverage via
    # 'build coverage' or similar and tests must already have been run.

    # Redirect lcov output into named pipe to avoid looping through it in a
    # subprocess.
    rm /tmp/coverageReportHtmlPipe &> /dev/null
    mkfifo /tmp/coverageReportHtmlPipe

    # Read generated coverage data for Xcalar binaries. Place in coverage.info.
    lcov --capture \
         --directory $XLRDIR \
         --output-file "${outputDir}/coverageTotal.info" \
        &> /tmp/coverageReportHtmlPipe &

    local failMessage=""
    while IFS= read -r line; do
        if [[ "$line" =~ .*ERROR.* ]]; then
            failMessage="lcov encountered an error. Please ensure that you ran \
tests that generate coverage data. For help, see \
http://wiki.int.xcalar.com/mediawiki/index.php/Code_Coverage"
        fi

        echo "$line"
    done < /tmp/coverageReportHtmlPipe

    rm /tmp/coverageReportHtmlPipe &> /dev/null

    if [ "$failMessage" != "" ]; then
        # lcov failed. Bail.
        fail "$failMessage" true
    fi

    # Remove coverage data for system includes.
    lcov --remove "${outputDir}/coverageTotal.info" \
         --output-file "${outputDir}/coverage.info" \
         "/usr/include/*"

    # Generate HTML output.
    genhtml "${outputDir}/coverage.info" --output-directory "${outputDir}/coverage"

    if [ "$launchBrowser" != false ]; then
        # Bring up browser to display result. Don't talk about it too much.
        sensible-browser "${outputDir}/coverage/index.html" & disown > /dev/null 2>&1
    fi

    echo ""
    echo "Output placed in ${outputDir}/coverage"
}


#
# Parse script parameters.
#

if [ "$#" = 0 ]; then
    fail
fi

# Values we may or may not get from parsing.
reportType=""
outputDir=""
launchBrowser=false

# Parse state
parseReportType=false
parseOutputDir=false

for arg in "$@"; do
    if [ "$parseReportType" != false ]; then
        reportType="$arg"
        parseReportType=false
        continue
    elif [ "$parseOutputDir" != false ]; then
        outputDir="$arg"
        parseOutputDir=false
        continue
    fi

    case "$arg" in
    --type)
        if [ "$reportType" != "" ]; then
            fail "--type parameter must occur exactly once."
        fi
        parseReportType=true
        ;;
    --browser)
        launchBrowser=true
        ;;
    --output)
        parseOutputDir=true
        ;;
    *)
        fail "Unrecognized option."
        ;;
    esac
done

# Validate parameters.
if [ "$reportType" = "" ]; then
    if [ "$parseReportType" != false ]; then
        fail "--type parameter requires an argument."
    else
        fail "--type parameter must occur exactly once."
    fi
fi

if [ "$outputDir" != "" ]; then
    if [ ! -d "$outputDir" ]; then
        if [ -f "$outputDir" ]; then
            fail "--output must be directory."
        fi
        
        mkdir "$outputDir"
        if [ "$?" != "0" ]; then
            fail "Failed to create output directory $outputDir" true
        fi
    fi
else
    outputDir="."
fi


#
# Delegate based on coverage report type.
#

case "$reportType" in
html)
    coverageReportHtml $outputDir $launchBrowser
    ;;
*)
    fail "Invalid report type ${reportType}."
    ;;
esac
