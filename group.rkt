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

(struct group [table by index]
  #:property prop:sequence
  (λ (g) 
    (let ([df (group-table g)])
      (sequence-map (λ (key indices)
                      (values key (table-with-index df (list->vector indices))))
                    (group-index g))))
  
  #:methods gen:custom-write
  [(define write-proc
     (λ (g port mode)
       (fprintf port "#<group by ~a [~a rows x ~a cols]>"
                (group-by g)
                (index-length (group-index g))
                (add1 (length (table-data (group-table g)))))))])

;; ----------------------------------------------------

(define (group-table/by df by)
  (match by
    [(list k)
     (let ([col (table-column df k)])
       (group (table-drop df (list k)) k (build-index col #f)))]
    [(list k ks ...)
     (group (table-drop df by)
            by
            (build-index (sequence-map cdr (table-cut df by)) #f))]))

;; ----------------------------------------------------

(define (group-fold proc init g #:result [result identity])
  (let* ([xs-columns (table-column-names (group-table g))]
         [xs-init (map (const init) xs-columns)])
    (for/table ([columns (cons (group-by g) xs-columns)])
               ([(key indices) (group-index g)])
      (cons key (for/fold ([xs xs-init] #:result (map result xs))
                          ([i indices])
                  (map proc xs (cdr (table-row (group-table g) i))))))))

;; ----------------------------------------------------

(define (group-count g)
  (group-fold (λ (a b) (if b (add1 a) a)) 0 g))

;; ----------------------------------------------------

(define (group-min g [less-than? sort-ascending])
  (group-fold (λ (a b) (if (or (not a) (less-than? a b)) a b)) #f g))

;; ----------------------------------------------------

(define (group-max g [less-than? sort-ascending])
  (group-fold (λ (a b) (if (or (not a) (less-than? a b)) b a)) #f g))

;; ----------------------------------------------------

(define (group-mean g)
  (let ([agg (λ (a b)
               (cond
                 [(not b) a]
                 [(not a) (list b 1)]
                 [else    (list (+ (first a) b)
                                (+ (second a) 1))]))])
    (group-fold agg #f g #:result (λ (pair) (apply / pair)))))

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
    (group-fold agg (set) g #:result set->list)))

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
    (group-fold agg (cons #f 0) g #:result car)))

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
  (define g (group-table/by birds '(bird)))

  ; TODO:
  )
