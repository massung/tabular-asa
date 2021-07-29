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
 table-cut
 table-drop
 table-row
 table-record
 table-records
 table-head
 table-tail
 table-select
 table-map
 table-filter
 table-drop-na
 table-sort
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
