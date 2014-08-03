#!/usr/bin/env bash

PREFIX=cofoja
VERSION=1.1-r150
URL=http://cofoja.googlecode.com/files/${PREFIX}-${VERSION}.jar

echo "For sources see http://code.google.com/p/cofoja/source/checkout" > \
${PREFIX}-${VERSION}-sources.txt
jar -cvf ${PREFIX}-${VERSION}-sources.jar ${PREFIX}-${VERSION}-sources.txt
rm ${PREFIX}-${VERSION}-sources.txt

echo "For documentation see http://code.google.com/p/cofoja/" > \
${PREFIX}-${VERSION}-javadoc.txt
jar -cvf ${PREFIX}-${VERSION}-javadoc.jar ${PREFIX}-${VERSION}-javadoc.txt
rm ${PREFIX}-${VERSION}-javadoc.txt

wget -nc $URL

echo "deploy ${PREFIX}-${VERSION}-SNAPSHOT by:"
echo "   mvn deploy:deploy-file -DrepositoryId=sonatype-nexus-snapshots -Dfile=${PREFIX}-${VERSION}.jar -Durl=https://oss.sonatype.org/content/repositories/snapshots -Dsources=${PREFIX}-${VERSION}-sources.jar -Djavadoc=${PREFIX}-${VERSION}-javadoc.jar -DpomFile=pom.xml -Dversion=${VERSION}-SNAPSHOT"
echo ""
echo "sign and deploy ${PREFIX}-${VERSION} to staging by:"
echo "   mvn gpg:sign-and-deploy-file -DrepositoryId=sonatype-nexus-staging -Dfile=${PREFIX}-${VERSION}.jar -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -Dsources=${PREFIX}-${VERSION}-sources.jar -Djavadoc=${PREFIX}-${VERSION}-javadoc.jar -DpomFile=pom.xml -Dversion=${VERSION} -Dgpg.keyname=_GPG_KEY_ID_"

