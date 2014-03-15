#!/usr/bin/env bash
#
# Script adapted from
# https://github.com/massie/adam/blob/a1825f06847ea781d75ea755b8086897e7b447a6/scripts/create-thirdparty-repo.sh

# This script packages the Hadoop-BAM dependencies so that they are ready
# to be ftp-ed to sourceforge.
# NOTE: requires to download and unzip Picard tools to /tmp prior
# to runing the script


TMPDIR=/tmp/hadoop_bam_dependency_repo

REPO_ID="hadoop-bam-repo"
OUTPUT_DIR="$TMPDIR/$REPO_ID"
OUTPUT_URL="file://$OUTPUT_DIR"
PICARD_VERSIONS=( "1.93" "1.107" )
SNAPSHOT_VERSION="6.1-SNAPSHOT"

rm -rf "$TMPDIR"
mkdir -p "$TMPDIR"

function fetch_and_build() {
    version=$1
    prefix=$2
    groupid=$3
    artifactid=$4

    DEPENDENCY_PATH="/tmp/picard-tools-$version" 

    jar_file=$(find $DEPENDENCY_PATH -name "${prefix}*.jar")
    if [[ -e $jar_file ]]; then
        jar_version=$(echo $(basename "$jar_file") | sed "s@${prefix}-\([0-9]*.[0-9]*\).jar@\1@g")
	echo "deploying prefix:$prefix groupid:$groupid artifactid:$artifactid jar_version:$jar_version"
	mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$jar_file \
            -DgroupId=${groupid} -DartifactId=${artifactid} -Dversion=$jar_version -DgeneratePom.description="$4"
    else
        echo "could not find $prefix jar for version $version"
    fi
}

for version in ${PICARD_VERSIONS[@]}; do
        echo "Processing version $version"
	fetch_and_build $version sam samtools samtools Samtools
	fetch_and_build $version picard picard picard Picard
	fetch_and_build $version tribble tribble tribble Tribble
	fetch_and_build $version variant variant variant Variant
done

