#!/bin/bash

luceneJarDir=/usr/lib64/python2.6/site-packages/lucene

javac -cp ${luceneJarDir}/lucene-core-4.3.0.jar:${luceneJarDir}/lucene-analyzers-common-4.3.0.jar org/meresco/lucene/MerescoStandardAnalyzer.java 