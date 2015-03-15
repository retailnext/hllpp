# hllpp

[![Build Status](https://travis-ci.org/retailnext/hllpp.svg)](https://travis-ci.org/retailnext/hllpp) [![GoDoc](https://godoc.org/github.com/retailnext/hllpp?status.svg)](https://godoc.org/github.com/retailnext/hllpp)

hllpp is an implementation of the HyperLogLog++ cardinality estimation algorithm in go. It optimizes for memory usage over CPU usage. It implements all the HyperLogLog optimizations introduced in the HyperLogLog++ paper (http://goo.gl/Z5Sqgu). Some notable features include:
* marshaling so you can serialize to your datastore
* extra space savings by only using 5 bits per register when possible
