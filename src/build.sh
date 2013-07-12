#!/bin/bash
mydir=$(cd $(dirname $0); pwd)
buildDir=$mydir/build
libDir=$mydir/lib

rm -rf $buildDir
mkdir $buildDir
test -d $libDir || mkdir -p $libDir

luceneJarDir=/usr/lib64/python2.6/site-packages/lucene
classpath=${luceneJarDir}/lucene-core-4.3.0.jar:${luceneJarDir}/lucene-analyzers-common-4.3.0.jar

javac -cp ${classpath} -d ${buildDir} org/meresco/lucene/MerescoStandardAnalyzer.java 
(cd $buildDir; jar -c org > $libDir/meresco-lucene.jar)
rm -rf $buildDir

python -m jcc.__main__ \
    --root $mydir/root \
    --use_full_names \
    --import lucene \
    --shared \
    --arch x86_64 \
    --jar $libDir/meresco-lucene.jar \
    --python meresco_lucene \
    --build \
    --install