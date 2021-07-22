#lang racket

#|

Racket Tables - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")
(require "column.rkt")
(require "group.rkt")
(require "table.rkt")
(require "read.rkt")
(require "print.rkt")
(require "write.rkt")

;; ----------------------------------------------------

(provide

 ; index
 index->stream
 index-length
 index-empty?
 index-ref
 index-for-each
 index-map
 index-filter
 index-head
 index-tail
 index-reverse
 index-sort

 ; column
 (struct-out column)
 column->stream
 column-length
 column-rename
 column-ref
 column-for-each
 column-map
 column-filter
 column-head
 column-tail
 column-reverse
 column-sort
 
 ; group
 (struct-out secondary-index)
 build-secondary-index
 secondary-index->stream
 secondary-index->index
 secondary-index-length
 secondary-index-empty?
 secondary-index-count
 secondary-index-find
 secondary-index-member
 secondary-index-ref
 secondary-index-min
 secondary-index-max
 secondary-index-mean
 secondary-index-median
 secondary-index-mode

 ; table
 (struct-out table)
 empty-table
 table-with-column
 table->row-stream
 table->record-stream
 table-shape
 table-length
 table-empty?
 table-index
 table-for-each
 table-map
 table-columns
 table-column-names
 table-row
 table-record
 table-head
 table-tail
 table-filter
 table-drop-na
 table-drop
 table-cut
 table-sort-by
 table-index-by
 table-with-index
 table-distinct
 table-reverse

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
