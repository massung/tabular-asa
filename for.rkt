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
    [(_ (init-forms ...) sequences body ... tail-expr)
     (with-syntax ([original stx]
                   [((pre-body ...) (post-body ...))
                    (split-for-body stx #'(body ... tail-expr))])
       #'(for/fold/derived original
           ([b (new table-builder% init-forms ...)] #:result (send b build))
           sequences
           pre-body ...

           ; each body result is a list/row of values
           (begin0 b (send b add-row (let () post-body ...)))))]))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; create a list of some names
  (define names '("Superman" "Batman" "Black Widow" "Wolverine" "Wonder Woman"))
  (define genders '(m m f m f))
  (define universes '("DC" "DC" "Marvel" "Marvel" "DC"))

  ; validates the table
  (define (verify-table df)
    (check-equal? (table-length df) (length names))
    (check-equal? (table-header df) '(name gender universe))
    (check-equal? (sequence->list (sequence-map cons df))
                  (for/list ([i (in-naturals)]
                             [name names]
                             [gender genders]
                             [universe universes])
                    (list i name gender universe))))

  ; create with rows
  (test-case "for/table - rows"
             (verify-table (for/table ([columns '(name gender universe)])
                             ([name names]
                              [gender genders]
                              [universe universes])
                             (list name gender universe))))

  ; create with records
  (test-case "for/table - records"
             (verify-table (for/table ([columns '(name gender universe)])
                             ([name names]
                              [gender genders]
                              [universe universes])
                             (hash 'name name
                                   'gender gender
                                   'universe universe)))))
