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

; find all authors using more than one publisher
(let ([df (~> books
              (table-cut '(Author Publisher))
              (table-drop-na)
              (group-table/by '(Author))
              (group-unique))])
  (table-select df (table-apply (λ (publishers)
                                  (> (length publishers) 1))
                                df
                                '(Publisher))))

; index the books by author and collect those rows
(let ([ix (build-index (table-column books 'Author))])
  (for*/list ([(author indices) (index-scan-keys ix #:from "J" #:to "R")]
              [i indices])
    (table-row books i)))

; plot how many books there are per genre
(let ([df (~> books
              (table-cut '(Genre Title))
              (group-table/by '(Genre))
              (group-count))])
  (plot (discrete-histogram (for/list ([x (table-column df 'Genre)]
                                       [y (table-column df 'Title)])
                              (list x y)))))
