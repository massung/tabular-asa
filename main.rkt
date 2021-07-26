#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")
(require "column.rkt")
(require "group.rkt")
(require "table.rkt")
(require "read.rkt")
(require "join.rkt")
(require "print.rkt")
(require "write.rkt")

;; ----------------------------------------------------

(provide

 ; index
 index->stream
 index-length
 index-empty?
 index-compact
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
 column-empty?
 column-equal?
 column-compact
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
 secondary-index-sorted?
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
 table->row-stream
 table->record-stream
 table-shape
 table-length
 table-empty?
 table-index
 table-compact
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
 table-join

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

(module+ test
  (require rackunit)

  ; load a simple table of books
  (define df (table-read/csv "test/books.csv"))

  ; ensure that the table loaded the correct size of data
  (check-equal? (call-with-values (λ () (table-shape df)) list) '(211 5))
  (check-equal? (table-column-names df) '(Title Author Genre Height Publisher))

  ; record streaming
  (let* ([record (stream-first (table->record-stream df #:keep-index? #f))]

         ; create a list of the record keys
         [header (hash-keys record)])
    (check-not-false (for/and ([h header])
                       (memq h (table-column-names df)))))
  
  ; remove all missing 
  (define df-na (table-drop-na df))

  ; ensure resulting size
  (check-equal? (table-length df-na) 112)

  ; head and tail
  (check-true (column-equal? (table-column (table-head df-na 5) 'Height) '(228 235 197 179 197)))
  (check-true (column-equal? (table-column (table-tail df-na 5) 'Height) '(213 228 197 172 197)))

  ; test filtering and mapping
  (let ([pubs (table-filter (λ (i p) (string=? p "Wiley")) df-na '(Publisher))])
    (check-equal? (table-length pubs) 2)
    (check-equal? (let ([authors (table-map (λ (i a) a) pubs '(Author))])
                    (stream->list authors))
                  '("Goswami, Jaideva" "Foreman, John")))

  ; define a table for joins
  (define pubs (let ([b (new table-builder%
                             [columns '(Publisher Sales)])])
                 (send b add-row '("Wiley" 10000))
                 (send b add-row '("Penguin" 20000))
                 (send b add-row '("FUBAR" 0))
                 (send b add-row '("Random House" 12000))
                 (send b build)))

  ; join to get the best book sales per publisher
  (let* ([top (table-distinct df 'Publisher)]
         [w/sales (table-join top pubs '(Publisher) string<? 'right)])
    (check-true (column-equal? (table-column w/sales 'Sales) '(0 20000 12000 10000)))))
