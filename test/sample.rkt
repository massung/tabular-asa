#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

;(require tabular-asa)
(require "../main.rkt")
(require plot)
(require threading)

;; helper function for displaying output

; load a CSV file into a dataframe
(define books (call-with-input-file "books.csv" table-read/csv))

; pick a random title per genre
(group-sample (group-table/by books '(Genre)))

; find the longest books by publisher
(~> books 
    (table-drop-na '(Publisher))
    (table-sort '(Height) sort-descending)
    (table-distinct '(Publisher)))

; count how many books of each genre by publisher
(~> books
    (table-cut '(Publisher Genre Title))
    (group-table/by '(Publisher Genre))
    (group-count))

; index the books by author and collect those rows
(let ([ix (build-index (table-column books 'Author))])
  (for*/list ([(author indices) (index-scan-keys ix #:from "J" #:to "R")]
              [i indices])
    (table-row books i)))
