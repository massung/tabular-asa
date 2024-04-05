#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "column.rkt")
(require "for.rkt")
(require "index.rkt")
(require "join.rkt")
(require "group.rkt")
(require "orderable.rkt")
(require "print.rkt")
(require "read.rkt")
(require "table.rkt")
(require "write.rkt")

;; ----------------------------------------------------

(provide

 ; orderable
 sort-ascending
 sort-descending
 orderable?
 
 ; column
 (struct-out column)
 empty-column
 build-column
 column-length
 column-empty?
 column-compact
 column-rename
 column-ref
 column-head
 column-tail
 column-reverse
 column-sort

 ; index
 (struct-out index)
 build-index
 empty-index
 index-scan-keys
 index-scan
 index-length
 index-empty?
 index-sorted?
 index-find
 index-member
 index-ref
 index-map
 index-min
 index-max
 index-median
 index-mode

 ; table
 (struct-out table)
 empty-table
 table-preview
 table-length
 table-shape
 table-empty?
 table-reindex
 table-with-index
 table-header
 table-columns
 table-column
 table-with-column
 table-with-columns-renamed
 table-cut
 table-drop
 table-irow
 table-row
 table-record
 table-rows
 table-records
 table-head
 table-tail
 table-select
 table-for-each
 table-map
 table-apply
 table-filter
 table-update
 table-fold
 table-groupby
 table-drop-na
 table-reverse
 table-sort
 table-distinct

 ; join
 table-join/inner
 table-join/outer

 ; group
 group-fold
 group-count
 group-min
 group-max
 group-mean
 group-sum
 group-product
 group-and
 group-or
 group-list
 group-unique
 group-nunique
 group-sample

 ; read
 table-builder%
 table-read/sequence
 table-read/columns
 table-read/csv
 table-read/json
 table-read/jsexpr

 ; for
 for/table

 ; print
 display-table
 print-table
 write-table

 ; write
 table-write/csv
 table-write/json
 table-write/string)

;; ----------------------------------------------------

(table-preview (let ([old-preview (table-preview)])
                 (λ (df port mode)
                   (display-table df port))))
