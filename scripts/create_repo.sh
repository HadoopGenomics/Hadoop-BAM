#!/usr/bin/env bash
#
# Script adapted from
# https://github.com/massie/adam/blob/a1825f06847ea781d75ea755b8086897e7b447a6/scripts/create-thirdparty-repo.sh

# This script downloads a number of Hadoop-BAM version and creates
# a maven repository inside TMPDIR that can be then uploaded to a
# webserver. It also checks out a given branch and builds a
# snapshot version.

TMPDIR=/tmp/hadoop_bam_repo

HADOOPBAM_VERSIONS=( "6.0" "6.1" )
SNAPSHOT_VERSION="6.2-SNAPSHOT"
SNAPSHOT_MAVEN_VERSION="6.2.0-SNAPSHOT"
SNAPSHOT_BRANCH=master

REPO_ID="hadoop-bam-repo"
OUTPUT_DIR="$TMPDIR/$REPO_ID"
OUTPUT_URL="file://$OUTPUT_DIR"

rm -rf "$TMPDIR"
mkdir -p "$TMPDIR"

function fetch_and_build() {
    version=$1
    prefix=$2
    groupid=$3
    artifactid=$4

    jar_file=$(find $TMPDIR/hadoop-bam-${version} -name "${prefix}-${version}.jar")
    echo "processing jar file $jar_file"
    if [[ -e $jar_file ]]; then
        jar_version=$(echo $(basename "$jar_file") | sed "s@${prefix}-\([0-9]*.[0-9]*\).jar@\1@g")
	echo "deploying prefix:$prefix groupid:$groupid artifactid:$artifactid jar_version:$jar_version"
	mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$jar_file \
            -DgroupId=${groupid} -DartifactId=${artifactid} -Dversion=$jar_version -DgeneratePom.description="$4"
    else
        echo "could not find $prefix jar for version $version"
    fi
}

for version in ${HADOOPBAM_VERSIONS[@]}; do
        echo "Processing version $version"
	HADOOP_BAM_PATH="$TMPDIR/hadoop-bam-${version}"

	$(cd $TMPDIR && \
	    wget http://sourceforge.net/projects/hadoop-bam/files/hadoop-bam-${version}.tar.gz > /dev/null 2>&1 && \
	    tar xvpzf hadoop-bam-${version}.tar.gz > /dev/null 2>&1 )

        fetch_and_build $version hadoop-bam fi.tkk.ics.hadoop.bam hadoop-bam Hadoop-BAM
done

echo "processing $SNAPSHOT_VERSION"

$(cd $TMPDIR && git clone -b $SNAPSHOT_BRANCH http://git.code.sf.net/p/hadoop-bam/code hadoop-bam-${SNAPSHOT_VERSION} > /dev/null 2>&1 )
$(cd "$TMPDIR/hadoop-bam-${SNAPSHOT_VERSION}" && mvn clean package > /dev/null 2>&1 )

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$TMPDIR/hadoop-bam-${SNAPSHOT_VERSION}/target/hadoop-bam-${SNAPSHOT_MAVEN_VERSION}.jar \
    -DgroupId=fi.tkk.ics.hadoop.bam -DartifactId=hadoop-bam -Dversion="$SNAPSHOT_VERSION" -DgeneratePom.description="Hadoop-BAM"
