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
  (let* ([other-columns (table-header other)]

         ; columns to drop are those that match name and order
         [drop-columns (for/list ([k-on on] [k-with with] #:when (eq? k-on k-with))
                         k-on)]

         ; columns existing in other need to be renamed
         [rename-map (for/hash ([k (table-header df)]
                                #:when (and (not (memq k on))
                                            (member k other-columns)))
                       (values k (string->symbol (format "~a~a" k suffix))))])
    (table-with-columns-renamed (table-drop df drop-columns) rename-map)))

;; ----------------------------------------------------

(define (table-join left right on ix merge [else #f])
  (let ([builder (new table-builder%
                      [initial-size (table-length left)]
                      [columns (append (table-header left)
                                       (table-header right))])])

    ; iterate over left table, join with right
    (for ([(i row) (table-cut left on)])
      (let ([ks (index-member ix row)])
        (if ks
            (let ([left-row (table-irow left i)])
              (for ([j (cdr ks)])
                (let ([right-row (table-row right j)])
                  (send builder add-row (merge left-row right-row)))))

            ; outer join condition
            (when else
              (let ([row (else (table-irow left i))])
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
                (let ([empty (map (const #f) (table-header right))])
                  (λ (left-row)
                    (append left-row empty))))))
