#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "compare.rkt")
(require "index.rkt")
(require "table.rkt")
(require "read.rkt")
(require "print.rkt")

;; ----------------------------------------------------

(provide table-join)

;; ----------------------------------------------------

(define (table-remapped df other suffix on drop?)
  (let* ([other-columns (table-column-names other)]

         ; columns existing in other need to be renamed
         [rename-map (for/hash ([k (table-column-names df)]
                                #:when (and (not (equal? k on))
                                            (member k other-columns)))
                       (values k (string->symbol (format "~a~a" k suffix))))])
    (table-with-columns-renamed (if drop? (table-drop df (list on)) df) rename-map)))

;; ----------------------------------------------------

(define (table-join df
                    other
                    on
                    [less-than? less-than?]
                    #:with [with on]
                    #:how [how 'inner]
                    #:left-suffix [l-suffix "-x"]
                    #:right-suffix [r-suffix "-y"])
  (let* ([left (table-remapped df other l-suffix on #f)]
         [right (table-remapped other df r-suffix with (eq? on with))]

         ; build the index for the right table
         [ix (build-index (table-column other with) less-than?)]

         ; pick the join operation to perform
         [join (case how
                 ((inner) inner-join)
                 ((left) left-join)
                 ((right) 1);right-join)
                 ((outer) 1);outer-join)
                 (else (error "Unknown join type:" how)))]

         ; create the table builder
         [builder (new table-builder%
                       [initial-size (table-length df)]
                       [columns (append (table-column-names left)
                                        (table-column-names right))])])

    ; execute the join and build the final result table
    (join builder left right on ix)
    (send builder build)))

;; ----------------------------------------------------

(define (inner-join builder left right on ix)
  (for ([row (table-cut left (list on))])
    (match row
      [(list il x)
       (when x
         (let ([indices (index-scan ix #:from x #:to x)])
           (unless (stream-empty? indices)
             (let ([left-row (cdr (table-row left il))])
               (for ([ir indices])
                 (let ([right-row (cdr (table-row right ir))])
                   (send builder add-row (append left-row right-row))))))))])))

;; ----------------------------------------------------

(define (left-join builder left right on ix)
  (let ([empty-right (map (const #f) (table-column-names right))])
    (for ([row (table-cut left (list on))])
      (match row
        [(list il x)
         (let ([left-row (cdr (table-row left il))]
               [indices (and x (index-scan ix #:from x #:to x))])
           (if (or (not indices) (stream-empty? indices))
               (send builder add-row (append left-row empty-right))
               (for ([ir indices])
                 (let ([right-row (cdr (table-row right ir))])
                   (send builder add-row (append left-row right-row))))))]))))

;; ----------------------------------------------------

(module+ test
  (require rackunit)
  (require "read.rkt")
  (require "write.rkt")

  ; load a table of data
  (define books (call-with-input-file "test/books.csv" table-read/csv))

  ; make another table to join with
  (define pubs (table #(0 1 2 3)
                      '((Publisher . #("Penguin" "Wiley" "Random House" "FOOBAR")))))

  ; perform a join
  (table-write/string (table-join pubs books 'Publisher #:how 'left))
  )
