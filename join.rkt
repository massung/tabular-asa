#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")
(require "table.rkt")
(require "read.rkt")
(require "print.rkt")

;; ----------------------------------------------------

(provide table-join)

;; ----------------------------------------------------

(define (join-columns left right l-suffix r-suffix [drop-columns '(#f)])
  (let ([right (remq* drop-columns right)])
    (values (for/list ([k left])
              (list k (if (not (memq k right))
                          k
                          (string->symbol (format "~a~a" k l-suffix)))))
            (for/list ([k right])
              (list k (if (not (memq k left))
                          k
                          (string->symbol (format "~a~a" k r-suffix))))))))

;; ----------------------------------------------------

(define (join-indexes left right less-than? on [with on])
  (values (table-index left on less-than?)
          (table-index right with less-than?)))

;; ----------------------------------------------------

(define (table-join df
                    other
                    on
                    less-than?
                    [how 'inner]
                    #:left-columns [left-columns (table-column-names df)]
                    #:right-columns [right-columns (table-column-names other)]
                    #:left-suffix [l-suffix "-x"]
                    #:right-suffix [r-suffix "-y"])
  (let-values ([(left-index right-index)
                (apply join-indexes df other less-than? on)])

    ; determine the join function to call
    (let ([join (case how
                  ((inner) inner-join)
                  ((left) left-join)
                  ((right) right-join)
                  ((outer) outer-join)
                  (else (error "Unknown join type:" how)))])

      ; get the columns of the left and right, renamed + dropped
      (let-values ([(l r) (join-columns left-columns
                                        right-columns
                                        l-suffix
                                        r-suffix
                                        (case how
                                          ((inner left) on)
                                          ((right outer) null)))])

        ; build the new left/right tables to join (with proper columns)
        (let* ([left (table-cut df l)]
               [right (table-cut other r)]

               ; create a new table-builder% for the join function
               [builder (new table-builder%
                             [initial-size (table-length df)]
                             [columns (append (table-column-names left)
                                              (table-column-names right))])])
          (join builder left right left-index right-index)

          ; build the table and return it
          (send builder build))))))

;; ----------------------------------------------------

(define (join l-ix r-ix on-equal on-less on-else)
  (let ([less-than? (secondary-index-less-than? l-ix)])
    (letrec ([join (λ (li ri)
                     (let ([x (and (< li (secondary-index-length l-ix))
                                   (vector-ref (secondary-index-keys l-ix) li))]
                           [y (and (< ri (secondary-index-length r-ix))
                                   (vector-ref (secondary-index-keys r-ix) ri))])
                       (cond
                         [(and x y (equal? (car x) (car y)))
                          (when on-equal
                            (on-equal (cdr x) (cdr y)))
                          (join (add1 li) (add1 ri))]

                         ; left < right?
                         [(and x (or (not y) (less-than? (car x) (car y))))
                          (when on-less
                            (on-less (cdr x)))
                          (join (add1 li) ri)]

                         ; right < left?
                         [(and y (or (not x) (less-than? (car y) (car x))))
                          (when on-else
                            (on-else (cdr y)))
                          (join li (add1 ri))])))])
      (join 0 0))))

;; ----------------------------------------------------

(define (row-merger builder left right)
  (λ (xs ys)
    (for* ([xi xs] [yi ys])
      (send builder
            add-row
            (append (table-row left xi #:keep-index? #f)
                    (table-row right yi #:keep-index? #f))))))

;; ----------------------------------------------------

(define (inner-join builder left right l-ix r-ix)
  (join l-ix r-ix (row-merger builder left right) #f #f))

;; ----------------------------------------------------

(define (left-join builder left right l-ix r-ix)
  (let* ([empty-row (map (const #f) (table-column-names right))]
         [on-less (λ (xs)
                    (for ([xi xs])
                      (send builder
                            add-row
                            (append (table-row left xi #:keep-index? #f) empty-row))))])
    (join l-ix r-ix (row-merger builder left right) on-less #f)))

;; ----------------------------------------------------

(define (right-join builder left right l-ix r-ix)
  (let* ([empty-row (map (const #f) (table-column-names left))]
         [on-else (λ (xs)
                    (for ([xi xs])
                      (send builder
                            add-row
                            (append empty-row (table-row right xi #:keep-index? #f)))))])
    (join l-ix r-ix (row-merger builder left right) #f on-else)))

;; ----------------------------------------------------

(define (outer-join builder left right l-ix r-ix)
  (let* ([left-empty (map (const #f) (table-column-names left))]
         [right-empty (map (const #f) (table-column-names right))]

         ; take all lefts that don't match
         [on-less (λ (xs)
                    (for ([xi xs])
                      (send builder
                            add-row
                            (append (table-row left xi #:keep-index? #f) right-empty))))]

         ; take all rights that don't match
         [on-else (λ (xs)
                    (for ([xi xs])
                      (send builder
                            add-row
                            (append left-empty (table-row right xi #:keep-index? #f)))))])
    (join l-ix r-ix (row-merger builder left right) on-less on-else)))
