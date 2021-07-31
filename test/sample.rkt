#lang racket

;(require tabular-asa)
(require "../main.rkt")

;; First, let's load a CSV file of books in memory...

(define books (call-with-input-file "books.csv" table-read/csv))

;; Find the longest book written by each author...

(let* ([df (table-sort (table-drop-na books '(Author)) 'Height >)]
       [df (table-distinct df 'Author)])
  (display-table (table-cut df '(Author Title Height)) #:keep-index? #f))

;; Find the most prolific publisher...

(let* ([ix (build-index (table-column books 'Publisher))]
       [df (table-read/sequence (index-count ix) '(Publisher N))])
  (display-table (table-head (table-sort df 'N >) 1)))
