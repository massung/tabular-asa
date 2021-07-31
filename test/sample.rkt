#lang racket

;(require tabular-asa)
(require "../main.rkt")
(require "../compare.rkt")

;; First, let's load a CSV file of books in memory...

(define books (call-with-input-file "books.csv" table-read/csv))

;; Sort the table by Author and Title

(let* ([df (table-sort books '(Author Title))])
  (display-table df))

;; Find the most prolific publisher...

;(let* ([ix (build-index (table-column books 'Publisher))]
;       [df (table-read/sequence (index-count ix) '(Publisher N))])
;  (display-table (table-head (table-sort df 'N >) 1)))
