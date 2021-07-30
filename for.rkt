#lang racket

(require (for-syntax syntax/for-body))

;; ----------------------------------------------------

(require "read.rkt")
(require "table.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define-syntax (for/table stx)
  (syntax-case stx ()
    [(_ (init-form ...) sequences body ... tail-expr)
     (with-syntax ([original stx]
                   [((pre-body ...) (post-body ...))
                    (split-for-body stx #'(body ... tail-expr))])
       #'(for/fold/derived original
           ([b (new table-builder% init-form ...)] #:result (send b build))
           sequences
           pre-body ...

           ; each body result is a list/row of values
           (let ([row (let () post-body ...)])
             (begin0 b (send b add-row row)))))]))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; create a list of some names
  (define names '("Superman" "Batman" "Black Widow" "Wolverine" "Wonder Woman"))

  ; create a simple table for the heroes
  (define heroes (for/table ([columns '(name length)])
                   ([name names])
                   (list name (string-length name))))

  ; ensure everything matches up
  (check-equal? (table-length heroes) (length names))
  (check-equal? (table-column-names heroes) '(name length))
  (check-equal? (sequence->list (table-column heroes 'length))
                (map string-length names)))
