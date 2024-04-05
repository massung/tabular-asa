#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "column.rkt")
(require "index.rkt")
(require "orderable.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct table [index data]
  #:property prop:sequence
  (λ (df)
    (sequence-map (λ (i)
                    (values i (for/list ([col (table-data df)])
                                (vector-ref (cdr col) i))))
                  (table-index df)))
  
  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (df port mode)
       ((table-preview) df port mode)))])

;; ----------------------------------------------------

(define table-preview
  (make-parameter (λ (df port mode)
                    (let-values ([(rows cols) (table-shape df)])
                      (fprintf port "#<table [~a rows x ~a cols]>" rows cols)))))

;; ----------------------------------------------------

(define empty-table (table #() '()))

;; ----------------------------------------------------

(define (table-length df)
  (vector-length (table-index df)))

;; ----------------------------------------------------

(define (table-shape df)
  (values (table-length df) (length (table-data df))))

;; ----------------------------------------------------

(define (table-empty? df)
  (or (empty? (table-data df))
      (zero? (table-length df))))

;; ----------------------------------------------------

(define (table-reindex df)
  (table (build-vector (table-length df) identity)
         (for/list ([col (table-data df)])
           (cons (car col)
                 (for/vector ([i (table-index df)])
                   (vector-ref (cdr col) i))))))

;; ----------------------------------------------------

(define (table-with-index df index)
  (struct-copy table
               df
               [index (for/vector ([i index]) i)]))

;; ----------------------------------------------------

(define (table-header df)
  (map car (table-data df)))

;; ----------------------------------------------------

(define (table-columns df)
  (for/list ([col (table-data df)])
    (match col
      [(cons k v)
       (column k (table-index df) v)])))

;; ----------------------------------------------------

(define (table-column df k #:as [as #f])
  (match (assq k (table-data df))
    [(cons name data)
     (column (or as name) (table-index df) data)]
    [else
     (error "Column not found:" k)]))

;; ----------------------------------------------------

(define (table-with-column df seq #:as [as #f])
  (unless as
    (set! as (gensym "col")))

  ; empty tables get an entirely new index
  (if (empty? (table-data df))
      (let ([col (build-column seq)])
        (table (column-index col)
               (list (cons as (column-data col)))))

      ; create a new data vector using the existing index
      (let ([v (make-vector (+ (vector-argmax identity (table-index df)) 1) #f)])
        (for ([i (table-index df)] [x seq])
          (vector-set! v i x))

        ; append the column name . data to the table
        (struct-copy table
                     df
                     [data (if (assq as (table-data df))
                               (map (λ (col)
                                      (if (eq? (car col) as)
                                          (cons as v)
                                          col))
                                    (table-data df))
                               (append (table-data df)
                                       (list (cons as v))))]))))

;; ----------------------------------------------------

(define (table-with-columns-renamed df rename-map)
  (struct-copy table
               df
               [data (for/list ([col (table-data df)])
                       (match col
                         [(cons k v)
                          (cons (hash-ref rename-map k k) v)]))]))

;; ----------------------------------------------------

(define (table-cut df ks)
  (struct-copy table
               df
               [data (remq* '(#f) (for/list ([k ks])
                                    (let ([col (assq k (table-data df))])
                                      (or col (error "Column not found:" k)))))]))

;; ----------------------------------------------------

(define (table-drop df ks)
  (struct-copy table
               df
               [data (filter-not (λ (col)
                                   (member (car col) ks))
                                 (table-data df))]))

;; ----------------------------------------------------

(define (table-irow df i)
  (for/list ([col (table-data df)])
    (vector-ref (cdr col) i)))

;; ----------------------------------------------------

(define (table-row df n)
  (table-irow df (vector-ref (table-index df) n)))

;; ----------------------------------------------------

(define (table-record df n)
  (for/hash ([k (table-data df)]
             [v (table-row df n)])
    (values (car k) v)))

;; ----------------------------------------------------

(define (table-rows df)
  (sequence-map (λ (i row) row) df))

;; ----------------------------------------------------

(define (table-records df)
  (let ([ks (table-header df)])
    (sequence-map (λ (i row)
                    (for/hash ([k ks] [v row])
                      (values k v)))
                  df)))

;; ----------------------------------------------------

(define (table-head df [n 10])
  (struct-copy table
               df
               [index (vector-head (table-index df) n)]))

;; ----------------------------------------------------

(define (table-tail df [n 10])
  (struct-copy table
               df
               [index (vector-tail (table-index df) n)]))

;; ----------------------------------------------------

(define (table-select df seq)
  (let ([ix (table-index df)])
    (struct-copy table
                 df
                 [index (for/vector ([i ix] [x seq] #:when x) i)])))
  
;; ----------------------------------------------------

(define (table-map df proc [ks #f])
  (sequence-map (λ (i row) (proc row)) (if ks (table-cut df ks) df)))

;; ----------------------------------------------------

(define (table-apply df proc [ks #f])
  (sequence-map (λ (i row) (apply proc row)) (if ks (table-cut df ks) df)))

;; ----------------------------------------------------

(define (table-for-each df proc [ks #f])
  (sequence-for-each (λ (i row)
                       (apply proc row))
                     (if ks (table-cut df ks) df)))

;; ----------------------------------------------------

(define (table-filter df pred [ks #f])
  (table-select df (table-apply df pred ks)))

;; ----------------------------------------------------

(define (table-update df k proc #:ignore-na? [ignore-na #t])
  (let ([f (λ (x) (and x (proc x)))])
    (table-with-column df (table-apply df f (list k)) #:as k)))

;; ----------------------------------------------------

(define (table-fold df proc init [result identity])
  (table #(0) (map (λ (k x)
                     (cons k (vector (result x))))
                   (table-header df)
                   (for/fold ([cols (map (const init) (table-data df))])
                             ([(i row) df])
                     (map proc cols row)))))

;; ----------------------------------------------------

(define (table-groupby df ks [less-than? sort-ascending])
  (let* ([cf (table-drop df ks)]

         ; create a list of all the unique groups
         [groups (make-hash)]
         [keys '()]

         ; the order of keys to output
         [group-indices (for ([(i key) (table-cut df ks)] #:when (all? key))
                          (hash-update! groups key (λ (ix)
                                                     (when (null? ix)
                                                       (set! keys (cons key keys)))
                                                     (cons i ix)) '()))])

    ; return each group and table
    (sequence-map (λ (key)
                    (let ([ix (reverse (hash-ref groups key))])
                      (values (map list ks key) (table-with-index cf ix))))

                  ; determine the order groups are returned in
                  (if less-than?
                      (sort keys less-than?)
                      (reverse keys)))))

;; ----------------------------------------------------

(define (table-drop-na df [ks #f])
  (table-select df (sequence-map (λ (i row) (all? row)) (if ks (table-cut df ks) df))))

;; ----------------------------------------------------

(define (table-reverse df)
  (struct-copy table
               df
               [index (vector-reverse (table-index df))]))

;; ----------------------------------------------------

(define (table-sort df [ks #f] [less-than? sort-ascending])
  (let ([cols (if ks (table-cut df ks) df)])
    (struct-copy table
                 df
                 [index (vector-sort (table-index df)
                                     less-than?
                                     #:cache-keys? #t
                                     #:key (λ (i) (table-irow cols i)))])))

;; ----------------------------------------------------

(define (table-distinct df [ks #f] [keep 'first])
  (let ([h (make-hash)])
    (for ([i (table-index df)]
          [n (in-naturals)]
          [(_ r) (if ks (table-cut df ks) df)])
      (case keep
        [(first) (hash-ref! h r (cons n i))]
        [(last)  (hash-set! h r (cons n i))]
        [(none)  (hash-update! h
                               r
                               (λ (x) (and (eq? x #t) (cons n i)))
                               #t)]))

    ; build the new table index
    (struct-copy table
                 df
                 [index (let ([indices (sort (remq* '(#f) (hash-values h))
                                             <
                                             #:key car)])
                          (for/vector ([i indices])
                            (cdr i)))])))
