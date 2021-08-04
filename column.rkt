#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")
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

(define (column-equal? col seq)
  (let ([xs (sequence->stream col)])
    (and (for/and ([y seq])
           (and (not (stream-empty? xs))
                (let ([x (stream-first xs)])
                  (set! xs (stream-rest xs))
                  (equal? x y))))
         (stream-empty? xs))))

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

  ; check column-equal?
  (check-true (column-equal? c '(0 1 2 3 4)))
  (check-false (column-equal? c '(0 1 2 3)))
  (check-false (column-equal? c '(0 1 2 3 4 5)))
  (check-false (column-equal? c '(0 1 a 2 3)))

  ; renaming
  (check-equal? (column-name (column-rename c 'bar)) 'bar)

  ; ensure length and data are identical
  (define (check-data c seq)
    (check-true (column-equal? c seq)))

  ; head, tail, etc.
  (check-data (column-head c 2) #(0 1))
  (check-data (column-tail c 2) #(3 4))

  ; compaction test
  (check-equal? (column-data (column-compact (column-head c 2))) #(0 1))

  ; sequence property test
  (for ([x c] [i (in-naturals)])
    (check-equal? i x))

  ; mapping, filtering, etc.
  (check-equal? (sequence->list (sequence-map add1 c)) '(1 2 3 4 5))
  (check-equal? (sequence->list (sequence-filter even? c)) '(0 2 4))
  (check-equal? (sequence-fold + 0 c) 10)

  ; sorting
  (check-equal? (sequence->list (column-sort c)) '(0 1 2 3 4)))
