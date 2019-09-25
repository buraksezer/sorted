# sorted
[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/buraksezer/sorted) [![Build Status](https://travis-ci.org/buraksezer/sorted.svg?branch=master)](https://travis-ci.org/buraksezer/sorted) [![Coverage](http://gocover.io/_badge/github.com/buraksezer/sorted)](http://gocover.io/github.com/buraksezer/sorted) [![Go Report Card](https://goreportcard.com/badge/github.com/buraksezer/sorted)](https://goreportcard.com/report/github.com/buraksezer/sorted) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

SortedSet and SortedMap with [skip list](https://en.wikipedia.org/wiki/Skip_list) for Go. It also supports
sorting by score.

## Rationale

It's designed to be used in [Olric](https://github.com/buraksezer/olric). I need a GC friendly SortedSet/SortedMap 
implementation which implements Load/Dump functions and my keys and values are byte slices. 

## Features

* Implemented with Skip list,
* Uses only one byte slice. It's GC friendly,
* Supports compaction: Frees claimed memory when it's needed,  
* Supports sorting keys by score,
* Implements Load/Dump methods to store whole data set or transfer over network.

## Limitations

* Keys and values are byte slices due to its design.

## Usage 

## Contributions
Please don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.:w

