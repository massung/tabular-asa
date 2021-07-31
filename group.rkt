#lang racket

(require "index.rkt")
(require "read.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct group [index]
  )
 
;; ----------------------------------------------------

;(define (group-count ix)
;  (for/table ([columns '(
;  (table-read/sequence (λ (k v) (list k (length v))) ix))
