#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")
(require "orderable.rkt")
(require "table.rkt")
(require "read.rkt")

;; ----------------------------------------------------

(provide (all-defined-out)
         (except-out table-remapped)
         (except-out table-join))

;; ----------------------------------------------------

(define (table-remapped df other suffix on with)
  (let* ([other-columns (table-column-names other)]

         ; columns to drop are those that match name and order
         [drop-columns (for/list ([k-on on] [k-with with] #:when (eq? k-on k-with))
                         k-on)]

         ; columns existing in other need to be renamed
         [rename-map (for/hash ([k (table-column-names df)]
                                #:when (and (not (memq k on))
                                            (member k other-columns)))
                       (values k (string->symbol (format "~a~a" k suffix))))])
    (table-with-columns-renamed (table-drop df drop-columns) rename-map)))

;; ----------------------------------------------------

(define (table-join left right on ix merge [else #f])
  (let ([builder (new table-builder%
                      [initial-size (table-length left)]
                      [columns (append (table-column-names left)
                                       (table-column-names right))])])

    ; iterate over left table, join with right
    (for ([(i row) (table-cut left on)])
      (let ([ks (index-member ix row)])
        (if ks
            (let ([left-row (table-row left i)])
              (for ([j (cdr ks)])
                (let ([right-row (table-row right j)])
                  (send builder add-row (merge left-row right-row)))))
            (when else
              (let ([row (else (table-row left i))])
                (send builder add-row row))))))

    ; build the table
    (send builder build)))

;; ----------------------------------------------------

(define (table-join/inner df other on [less-than? sort-ascending] #:with [with on])
  (let ([left (table-remapped df other "-x" on '())]
        [right (table-remapped other df "-y" with on)]
        [ix (build-index (table-rows (table-cut other with)) less-than?)])
    (table-join left
                right
                on
                ix
                append)))

;; ----------------------------------------------------

(define (table-join/outer df other on [less-than? sort-ascending] #:with [with on])
  (let ([left (table-remapped df other "-x" on '())]
        [right (table-remapped other df "-y" with on)]
        [ix (build-index (table-rows (table-cut other with)) less-than?)])
    (table-join left
                right
                on
                ix
                append
                (let ([empty (map (const #f) (table-column-names right))])
                  (λ (left-row)
                    (append left-row empty))))))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; create a simple table
  (define people
    (let ([builder (new table-builder% [columns '(name age gender)])])
      (send builder add-row '("Jeff" 32 m))
      (send builder add-row '("Dave" 26 m))
      (send builder add-row '("Henry" 18 m))
      (send builder add-row '("Sally" 37 f))
      (send builder build)))
  
  ; create another table to join with
  (define jobs
    (let ([builder (new table-builder% [columns '(name title)])])
      (send builder add-row '("Jeff" janitor))
      (send builder add-row '("Sally" manager))
      (send builder add-row '("Dave" programmer))
      (send builder add-row '("Mary" vp))
      (send builder build)))

  ; join inner and outer
  (define inner (table-join/inner people jobs '(name)))
  (define outer (table-join/outer people jobs '(name)))
  
  ; verify join results
  (check-equal? (sequence->list (table-column inner 'title))
                '(janitor programmer manager))
  (check-equal? (sequence->list (table-column outer 'title))
                '(janitor programmer #f manager)))
