#lang racket

(provide (all-defined-out))

;; ----------------------------------------------------

(define (stream-zip xs)
  (match xs
    [(list x)
     (for/stream ([e x]) (list e))]
    [(list x xs ...)
     (for/stream ([e x] [e* (stream-zip xs)])
       (cons e e*))]))

;; ----------------------------------------------------

(define (all? . xs)
  (for/and ([x xs]) x))

;; ----------------------------------------------------

(define (any? . xs)
  (for/or ([x xs]) x))
