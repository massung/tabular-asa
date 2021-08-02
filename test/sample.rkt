#lang racket

;(require tabular-asa)
(require "../main.rkt")
(require "../compare.rkt")

;; First, let's load a CSV file of books in memory

(define books (call-with-input-file "books.csv" table-read/csv))

;; helper function for displaying output

(define (show-table title df)
  (displayln title)
  (displayln (make-string (string-length title) #\-))
  (display-table df)
  (newline))

;; Sorting by column(s)

(let ([df (table-sort books '(Author Title))])
  (show-table "Books sorted by Author and Title..." df))

;; Grouping and aggregating

(let ([g (table-group (table-cut books '(Publisher Title)) 'Publisher)])
  (show-table "Number of titles by publisher..." (table-sort (group-count g) '(Title)))
  (show-table "Random title per publisher..." (group-sample g)))

; Filtering

(let ([df (table-sort books '(Height) descending?)])
  (show-table "Largest book by genre..." (table-distinct df '(Genre))))

;(define pub-group (

;(define pub-count (group-count (table-group books '(Publisher))))

;(let* ([ix (build-index (table-column books 'Publisher))]
;       [df (table-read/sequence (index-count ix) '(Publisher N))])
;  (display-table (table-head (table-sort df 'N >) 1)))
