#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "column.rkt")
(require "for.rkt")
(require "index.rkt")
(require "orderable.rkt")
(require "read.rkt")
(require "table.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct group [tables columns index]
  #:property prop:sequence
  (λ (g)
    (let ([keys (sequence-map (λ (key _) key) (group-index g))])
      (in-parallel keys (group-tables g))))
  
  #:methods gen:custom-write
  [(define write-proc
     (λ (g port mode)
       (fprintf port "#<group [~a rows x ~a cols]>"
                (index-length (group-index g))
                (length (group-columns g)))))])

;; ----------------------------------------------------

(define (table-groupby df by #:as [as #f])
  (let* ([cf (table-drop df by)]

         ; key column
         [key (string->symbol (string-join (map ~a by) "-"))]

         ; values to partition by
         [seq (if (empty? (cdr by))
                  (table-column df (car by))
                  (table-rows (table-cut df by)))]

         ; create an unsorted index
         [ix (build-index seq #f)]

         ; generate the resulting tables    
         [tables (sequence-map (λ (k i) (table-with-index cf i)) ix)])
    (group tables (cons (or as key) (table-column-names cf)) ix)))
  
;; ----------------------------------------------------

(define (group-fold proc init g [result identity])
  (let ([ix (group-index g)])
    (for/table ([initial-size (index-length ix)]
                [columns (group-columns g)])
               ([(key df) g])
      (cons key (table-row (table-fold proc init df result) 0)))))

;; ----------------------------------------------------

(define (group-count g)
  (group-fold (λ (a b) (if b (add1 a) a)) 0 g))

;; ----------------------------------------------------

(define (group-min g [less-than? sort-ascending])
  (group-fold (λ (a b) (if (or (not b) (less-than? a b)) a b)) #f g))

;; ----------------------------------------------------

(define (group-max g [greater-than? sort-descending])
  (group-fold (λ (a b) (if (or (not a) (greater-than? b a)) b a)) #f g))

;; ----------------------------------------------------

(define (group-mean g)
  (let ([agg (λ (a b)
               (cond
                 [(not b) a]
                 [(not a) (list b 1)]
                 [else    (list (+ (first a) b)
                                (+ (second a) 1))]))])
    (group-fold agg #f g (λ (pair) (apply / pair)))))

;; ----------------------------------------------------

(define (group-sum g)
  (group-fold (λ (a b) (if b (+ a b) a)) 0 g))

;; ----------------------------------------------------

(define (group-product g)
  (group-fold (λ (a b) (if b (* a b) a)) 1 g))

;; ----------------------------------------------------

(define (group-and g)
  (group-fold (λ (a b) (and a b #t)) #t g))

;; ----------------------------------------------------

(define (group-or g)
  (group-fold (λ (a b) (or a (not (false? b)))) #f g))

;; ----------------------------------------------------

(define (group-list g)
  (let ([agg (λ (a b)
               (if (not b)
                   a
                   (cons b a)))])
    (group-fold agg '() g)))

;; ----------------------------------------------------

(define (group-unique g)
  (let ([agg (λ (a b)
               (if (not b)
                   a
                   (set-add a b)))])
    (group-fold agg (set) g set->list)))

;; ----------------------------------------------------

(define (group-nunique g)
  (let ([df (group-unique g)])
    (for/table ([columns (table-column-names df)])
               ([row df])
      (cons (second row) (map length (cddr row))))))

;; ----------------------------------------------------

(define (group-sample g)
  (let ([agg (λ (a b)
               (if (not b)
                   a
                   (match a
                     [(cons x n)
                      (let ([m (add1 n)])
                        (cons (if (zero? (random m)) b x) m))])))])
    (group-fold agg (cons #f 0) g car)))

;; ----------------------------------------------------

(module+ test
  (require rackunit)
  (require "print.rkt")

  ; create a simple table
  (define birds (table-read/sequence '(("Crane" 4.3 7.3)
                                       ("Crane" 5.2 7.5)
                                       ("Egret" 2.6 4.3)
                                       ("Egret" #f 5.5)
                                       ("Heron" 3.2 5.5)
                                       ("Heron" 5.5 6.6)
                                       ("Heron" 4.2 #f)
                                       ("Stork" 3.3 5.0)
                                       ("Stork" 3.8 5.0))
                                     '(bird length wingspan)))

  ; group by bird
  (define g (table-groupby birds '(bird)))

  ; TODO:
  )
