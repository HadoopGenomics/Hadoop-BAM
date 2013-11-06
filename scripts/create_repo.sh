#!/usr/bin/env bash
#
# Script adapted from
# https://github.com/massie/adam/blob/a1825f06847ea781d75ea755b8086897e7b447a6/scripts/create-thirdparty-repo.sh

TMPDIR=/tmp/hadoop_bam_repo

HADOOPBAM_VERSION="6.0"
PICARD_VERSION="1.93"
SAMTOOLS_VERSION="$PICARD_VERSION"

rm -rf "$TMPDIR"
mkdir -p "$TMPDIR"
$(cd $TMPDIR && wget http://sourceforge.net/projects/hadoop-bam/files/hadoop-bam-${HADOOPBAM_VERSION}.tar.gz)
$(cd "$TMPDIR" && tar xvpzf hadoop-bam-${HADOOPBAM_VERSION}.tar.gz)

REPO_ID="hadoop-bam-repo"
OUTPUT_DIR="$TMPDIR/$REPO_ID"
HADOOP_BAM_PATH="$TMPDIR/hadoop-bam-${HADOOPBAM_VERSION}"
# Delete any old artifacts
rm -rf $OUTPUT_DIR
OUTPUT_URL="file://$OUTPUT_DIR"

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$HADOOP_BAM_PATH/picard-$PICARD_VERSION.jar \
-DgroupId=picard -DartifactId=picard -Dversion=$PICARD_VERSION -DgeneratePom.description="Picard"

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$HADOOP_BAM_PATH/tribble-$PICARD_VERSION.jar \
-DgroupId=picard -DartifactId=tribble -Dversion=$PICARD_VERSION -DgeneratePom.description="Picard - Tribble"

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$HADOOP_BAM_PATH/variant-$PICARD_VERSION.jar \
-DgroupId=picard -DartifactId=variant -Dversion=$PICARD_VERSION -DgeneratePom.description="Picard - Variant"

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$HADOOP_BAM_PATH/sam-$SAMTOOLS_VERSION.jar \
-DgroupId=samtools -DartifactId=samtools -Dversion=$SAMTOOLS_VERSION -DgeneratePom.description="Samtools"

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$HADOOP_BAM_PATH/hadoop-bam-${HADOOPBAM_VERSION}.jar \
-DgroupId=fi.tkk.ics.hadoop.bam -DartifactId=hadoop-bam -Dversion=$HADOOPBAM_VERSION -DgeneratePom.description="Hadoop-BAM"

for i in $(find $OUTPUT_DIR -type d); do
    echo "Options +Indexes" > ${i}/.htaccess;
done

echo "now please push the contents of $OUTPUT_DIR to /home/project-web/hadoop-bam/htdocs/maven on sourceforge"

