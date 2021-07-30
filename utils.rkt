#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require racket/date)

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define (sequence-zip xs)
  (match xs
    [(list x)
     (for/stream ([e x]) (list e))]
    [(list x xs ...)
     (for/stream ([e x] [e* (sequence-zip xs)])
       (cons e e*))]))

;; ----------------------------------------------------

(define (all? xs)
  (for/and ([x xs]) x))

;; ----------------------------------------------------

(define (any? xs)
  (for/or ([x xs]) x))

;; ----------------------------------------------------

(define (vector-reverse v)
  (let ([n (vector-length v)])
    (build-vector n (λ (i) (vector-ref v (- n i 1))))))

;; ----------------------------------------------------

(define (vector-head v n)
  (vector-take v (min (vector-length v) n)))

;; ----------------------------------------------------

(define (vector-tail v n)
  (vector-take-right v (min (vector-length v) n)))
