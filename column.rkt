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

(define (column-length col)
  (index-length (column-index col)))

;; ----------------------------------------------------

(define (column-renamed col [name #f])
  (struct-copy column
               col
               [name (or name (gensym "col"))]))

;; ----------------------------------------------------

(define (column-ref col n)
  (index-ref (column-index col) (column-data col) n))

;; ----------------------------------------------------

(define (column-map proc col)
  (index-map proc (column-index col) (column-data col)))

;; ----------------------------------------------------

(define (column-filter proc col)
  (index-filter proc (column-index col) (column-data col)))

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
