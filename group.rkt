#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "orderable.rkt")
(require "read.rkt")
(require "table.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))
  
;; ----------------------------------------------------

(define (group-fold proc init g [result identity])
  (let ([builder (new table-builder%)])
    (for ([(keys df) g])
      (let ([row (table-row (table-fold df proc init result) 0)])
        (send builder
              add-row
              (append (map second keys) row)
              (append (map first keys) (table-header df)))))

    ; return the final table
    (send builder build)))

;; ----------------------------------------------------

(define (group-count g)
  (group-fold (λ (a b) (if b (add1 a) a)) 0 g))

;; ----------------------------------------------------

(define (group-min g [less-than? sort-ascending])
  (group-fold (λ (a b) (if (or (not b) (less-than? a b)) a b)) #f g))

;; ----------------------------------------------------

(define (group-max g [greater-than? sort-descending])
  (group-fold (λ (a b) (if (or (not a) (greater-than? b a)) b a)) #f g))

;; ----------------------------------------------------

(define (group-mean g)
  (let ([agg (λ (a b)
               (cond
                 [(not b) a]
                 [(not a) (list b 1)]
                 [else    (list (+ (first a) b)
                                (+ (second a) 1))]))])
    (group-fold agg #f g (λ (pair) (apply / pair)))))

;; ----------------------------------------------------

(define (group-sum g)
  (group-fold (λ (a b) (if b (+ a b) a)) 0 g))

;; ----------------------------------------------------

(define (group-product g)
  (group-fold (λ (a b) (if b (* a b) a)) 1 g))

;; ----------------------------------------------------

(define (group-and g)
  (group-fold (λ (a b) (and a b #t)) #t g))

;; ----------------------------------------------------

(define (group-or g)
  (group-fold (λ (a b) (or a (not (false? b)))) #f g))

;; ----------------------------------------------------

(define (group-list g)
  (let ([agg (λ (a b)
               (if (not b)
                   a
                   (cons b a)))])
    (group-fold agg '() g)))

;; ----------------------------------------------------

(define (group-unique g)
  (let ([agg (λ (a b)
               (if (not b)
                   a
                   (set-add a b)))])
    (group-fold agg (set) g set->list)))

;; ----------------------------------------------------

(define (group-nunique g)
  (let ([agg (λ (a b)
               (if (not b)
                   a
                   (set-add a b)))])
    (group-fold agg (set) g set-count)))

;; ----------------------------------------------------

(define (group-sample g)
  (let ([agg (λ (a b)
               (if (not b)
                   a
                   (match a
                     [(cons x n)
                      (let ([m (add1 n)])
                        (cons (if (zero? (random m)) b x) m))])))])
    (group-fold agg (cons #f 0) g car)))
