#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "column.rkt")
(require "index.rkt")
(require "table.rkt")
(require "read.rkt")
(require "join.rkt")
(require "print.rkt")
(require "write.rkt")

;; ----------------------------------------------------

(provide

 ; index
 (struct-out index)
 build-index
 empty-index
 index-scan
 index-length
 index-empty?
 index-find
 index-member
 index-ref
 index-map
 index-min
 index-max
 index-median
 index-mode

 ; column
 (struct-out column)
 column-length
 column-empty?
 column-equal?
 column-compact
 column-rename
 column-ref
 column-head
 column-tail
 column-reverse
 column-sort

 ; table
 (struct-out table)
 empty-table
 table-length
 table-shape
 table-empty?
 table-reindex
 table-columns
 table-column-names
 table-column
 table-with-column
 table-with-columns-renamed
 table-cut
 table-drop
 table-row
 table-record
 table-records
 table-head
 table-tail
 table-select
 table-for-each
 table-map
 table-filter
 table-drop-na
 table-sort
 table-distinct
 table-reverse

 ; join
 table-join/inner
 table-join/outer

 ; read
 table-builder%
 table-read/csv
 table-read/json

 ; print
 display-table
 print-table
 write-table

 ; write
 table-write/csv
 table-write/json
 table-write/string)
