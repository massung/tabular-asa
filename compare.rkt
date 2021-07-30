#lang racket

(require racket/generic)

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define-generics comparable
  (less-than? comparable other)
  #:fast-defaults
  ([boolean? (define (less-than? a b) (and a (not b)))]
   [number? (define (less-than? a b) (or (not b) (< a b)))]
   [string? (define (less-than? a b) (or (not b) (string<? a b)))]
   [char? (define (less-than? a b) (or (not b) (char<? a b)))]
   [sequence? (define (less-than? a b) (sequence<? a b))]))

;; ----------------------------------------------------

(define (sequence<? xs ys)
  (for/and ([x xs] [y ys])
    (less-than? x y)))
