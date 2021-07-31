#lang racket

(require "index.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define (index-count ix)
  (sequence-map (λ (k v) (list k (length v))) ix))
