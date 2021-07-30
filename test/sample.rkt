#lang racket

;(require tabular-asa)
(require "../main.rkt")

;; First, let's load a CSV file of books in memory...

(define books (call-with-input-file "books.csv" table-read/csv))

;; Keep only the short, fiction books...

(define short-books (let ([pred (λ (index height genre)
                                  (and (< height 200)
                                       (string-ci=? genre "fiction")))])
                      (table-filter pred books '(Height Genre))))

;; Sort them by title...

(define sorted (table-sort short-books 'Title))

;; Take the top 20...

(define top (table-head sorted 20))

;; Index the results by publisher...

(define ix (build-index (table-column top 'Publisher)))

;; Determine which publisher has the most top books

(displayln (for/fold ([pub #f] [n 0] #:result pub)
                     ([(publisher indices) ix])
             (let ([m (length indices)])
               (if (> m n)
                   (values publisher m)
                   (values pub n)))))
