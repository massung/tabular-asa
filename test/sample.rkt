#lang racket

;(require tabular-asa)
(require "../main.rkt")

;; helper function for displaying output

(define (show-table title df)
  (displayln title)
  (displayln (make-string (string-length title) #\-))
  (display-table df)
  (newline)
  (newline))

;; First, let's load a CSV file of books in memory

(define books (call-with-input-file "books.csv" table-read/csv))
(show-table "All books..." books)

;; Sorting by column(s)

(let ([df (table-sort (table-drop-na books '(Author Title)) '(Author Title))])
  (show-table "Books sorted by Author and Title (missing removed)..." df))

;; Grouping and aggregating

(let ([g (table-group (table-cut books '(Publisher Title)) 'Publisher)])
  (show-table "Number of titles by publisher..." (table-sort (group-count g) '(Title) sort-descending))
  (show-table "Random title per publisher..." (group-sample g)))

; Filtering

(let ([df (table-sort books '(Height) sort-descending)])
  (show-table "Largest book by genre..." (table-distinct df '(Genre))))
