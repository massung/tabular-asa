#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct column [name index data]
  #:property prop:sequence
  (λ (col) (column->stream col))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (col port mode)
       (fprintf port "#<column ~a [~a values]>"
                (column-name col)
                (column-length col))))])

;; ----------------------------------------------------

(define (column->stream col)
  (index->stream (column-index col) (column-data col)))

;; ----------------------------------------------------

(define empty-column (column 'emtpy empty-index #()))

;; ----------------------------------------------------

(define (build-index-column n #:name [name '||])
  (let ([ix (build-index n)])
    (column name ix ix)))

;; ----------------------------------------------------

(define (build-column seq #:name [name #f])
  (let ([data (for/vector ([x seq]) x)])
    (column (or name (gensym "col")) (build-index (vector-length data)) data)))

;; ----------------------------------------------------

(define (column-length col)
  (index-length (column-index col)))

;; ----------------------------------------------------

(define (column-empty? col)
  (zero? (column-length col)))

;; ----------------------------------------------------

(define (column-equal? col seq)
  (call/ec (λ (return)
             (do ([s1 (column->stream col) (stream-rest s1)]
                  [s2 (sequence->stream seq) (stream-rest s2)])
               [(or (stream-empty? s1) (stream-empty? s2))
                (and (stream-empty? s1) (stream-empty? s2))]
               (unless (equal? (stream-first s1)
                               (stream-first s2))
                 (return #f))))))

;; ----------------------------------------------------

(define (column-compact col)
  (let ([ix (column-index col)])
    (struct-copy column
                 col
                 [index (build-index (index-length ix))]
                 [data (index-compact ix (column-data col))])))

;; ----------------------------------------------------

(define (column-rename col [name #f])
  (struct-copy column
               col
               [name (or name (gensym "col"))]))

;; ----------------------------------------------------

(define (column-ref col n)
  (index-ref (column-index col) (column-data col) n))

;; ----------------------------------------------------

(define (column-for-each proc col)
  (index-for-each proc (column-index col) (column-data col)))

;; ----------------------------------------------------

(define (column-map proc col)
  (struct-copy column
               col
               [data (index-map proc (column-index col) (column-data col))]))

;; ----------------------------------------------------

(define (column-filter proc col)
  (struct-copy column
               col
               [index (index-filter proc (column-index col) (column-data col))]))

;; ----------------------------------------------------

(define (column-head col [n 10])
  (struct-copy column
               col
               [index (index-head (column-index col) n)]))

;; ----------------------------------------------------

(define (column-tail col [n 10])
  (struct-copy column
               col
               [index (index-tail (column-index col) n)]))

;; ----------------------------------------------------

(define (column-reverse col)
  (struct-copy column
               col
               [index (index-reverse (column-index col))]))

;; ----------------------------------------------------

(define (column-sort col less-than?)
  (struct-copy column
               col
               [index (index-sort (column-index col) (column-data col) less-than?)]))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; create a simple column
  (define c (build-column (range 5) #:name 'foo))

  ; verify the column
  (check-equal? (column-name c) 'foo)
  (check-equal? (column-index c) #(0 1 2 3 4))
  (check-equal? (column-data c) #(0 1 2 3 4))
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

  ; iteration and indexing
  (column-for-each (λ (i x) (check-equal? i x)) c)

  ; map, filter, reverse, sort, etc.
  (check-data (column-map (λ (i x) (* 10 x)) c) #(0 10 20 30 40))
  (check-data (column-filter (λ (i x) (even? x)) c) #(0 2 4))
  (check-data (column-reverse c) #(4 3 2 1 0))
  (check-data (column-sort c >) #(4 3 2 1 0)))
