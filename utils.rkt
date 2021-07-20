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
