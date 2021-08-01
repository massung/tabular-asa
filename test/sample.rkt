#lang racket

;(require tabular-asa)
(require "../main.rkt")

;; First, let's load a CSV file of books in memory

(define books (call-with-input-file "books.csv" table-read/csv))

;; Sort the table by Author and Title

(define sorted (table-sort books '(Author Title)))

;; Add a column that's the Publisher and Author combined
(define w/pub-auth (table-with-column (table-drop books '(Publisher Author))
                                      (table-map cdr (table-cut books '(Publisher Author)))
                                      #:as 'Pub-Author))

;; Find the most prolific publisher...

;(define pub-count (group-count (table-group books '(Publisher))))

;(let* ([ix (build-index (table-column books 'Publisher))]
;       [df (table-read/sequence (index-count ix) '(Publisher N))])
;  (display-table (table-head (table-sort df 'N >) 1)))
