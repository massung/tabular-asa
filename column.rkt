#lang racket

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
  (index-map proc (column-index col) (column-data col)))

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
