#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "orderable.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct column [name index data]
  #:property prop:sequence
  (λ (col)
    (let ([ix (column-index col)]
          [data (column-data col)])
      (sequence-map (λ (i) (vector-ref data i)) ix)))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (col port mode)
       (fprintf port "#<column ~a [~a values]>"
                (column-name col)
                (column-length col))))])

;; ----------------------------------------------------

(define empty-column (column '|| #() #()))

;; ----------------------------------------------------

(define (build-column seq #:as [as #f])
  (let* ([data (for/vector ([x seq]) x)]
         [index (build-vector (vector-length data) identity)])
    (column (or as (gensym "col")) index data)))

;; ----------------------------------------------------

(define (column-length col)
  (vector-length (column-index col)))

;; ----------------------------------------------------

(define (column-empty? col)
  (vector-empty? (column-index col)))

;; ----------------------------------------------------

(define (column-compact col)
  (struct-copy column
               col
               [data (for/vector ([x col]) x)]))

;; ----------------------------------------------------

(define (column-rename col [as #f])
  (struct-copy column
               col
               [name (or as (gensym "col"))]))

;; ----------------------------------------------------

(define (column-ref col n)
  (vector-ref (column-data col) (vector-ref (column-index col) n)))

;; ----------------------------------------------------

(define (column-head col [n 10])
  (struct-copy column
               col
               [index (vector-head (column-index col) n)]))

;; ----------------------------------------------------

(define (column-tail col [n 10])
  (struct-copy column
               col
               [index (vector-tail (column-index col) n)]))

;; ----------------------------------------------------

(define (column-reverse col)
  (struct-copy column
               col
               [index (vector-reverse (column-index col))]))

;; ----------------------------------------------------

(define (column-sort col [less-than? sort-ascending])
  (let ([ix (column-index col)]
        [data (column-data col)])
    (struct-copy column
                 col
                 [index (vector-sort ix
                                     less-than?
                                     #:key (λ (i) (vector-ref data i)))])))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; create a simple column
  (define c (build-column 5 #:as 'foo))

  ; verify the column
  (check-equal? (column-name c) 'foo)
  (check-equal? (column-length c) 5)
  (check-equal? (column-empty? c) #f)
  (check-equal? (sequence->list c) '(0 1 2 3 4))

  ; renaming check
  (check-equal? (column-name (column-rename c 'bar)) 'bar)

  ; head and tail
  (check-equal? (sequence->list (column-head c 2)) '(0 1))
  (check-equal? (sequence->list (column-tail c 2)) '(3 4))

  ; sorting
  (check-equal? (sequence->list (column-sort c sort-descending)) '(4 3 2 1 0)))
