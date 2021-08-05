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
       (let-values ([(rows cols) (table-shape df)])
         (fprintf port "#<table [~a rows x ~a cols]>" rows cols))))])

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

(define (table-columns df)
  (for/list ([col (table-data df)])
    (match col
      [(cons k v)
       (column k (table-index df) v)])))

;; ----------------------------------------------------

(define (table-column-names df)
  (map car (table-data df)))

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
  (let ([ks (table-column-names df)])
    (sequence-map (λ (row)
                    (for/hash ([k ks] [v (cdr row)])
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

(define (table-for-each proc df [ks #f])
  (sequence-for-each (λ (i row)
                       (apply proc row))
                     (if ks (table-cut df ks) df)))
  
;; ----------------------------------------------------

(define (table-map proc df [ks #f])
  (sequence-map (λ (i row) (proc row)) (if ks (table-cut df ks) df)))

;; ----------------------------------------------------

(define (table-apply proc df [ks #f])
  (sequence-map (λ (i row) (apply proc row)) (if ks (table-cut df ks) df)))

;; ----------------------------------------------------

(define (table-filter pred df [ks #f])
  (table-select df (table-apply pred df ks)))

;; ----------------------------------------------------

(define (table-fold proc init df [result identity])
  (table #(0) (for/list ([k (table-data df)]
                         [x (sequence-fold (λ (acc row)
                                             (for/list ([x acc]
                                                        [y row])
                                               (proc x y)))
                                           (map (const init)
                                                (table-data df))
                                           (table-rows df))])
                (cons (car k) (vector (result x))))))

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

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; create a simple table by hand
  (define df (table (build-vector 5 identity)
                    (list (cons 'name #("Jeff" "Jennie" "Isabel" "Dave" "Bob"))
                          (cons 'age #(44 39 12 24 38))
                          (cons 'gender #(m f f m m)))))

  ; test shape/size
  (check-equal? (table-length df) 5)
  (check-equal? (length (table-data df)) 3)

  ; test unknown column
  (check-exn exn:fail? (λ () (table-column df 'foo)))

  ; check row/record conversion
  (check-equal? (table-row df 0) '("Jeff" 44 m))
  (check-equal? (table-record df 0) #hash((name . "Jeff") (age . 44) (gender . m)))

  ; check dropping, cutting and re-ordering of columns
  (check-equal? (table-column-names (table-cut df '(age name))) '(age name))
  (check-equal? (table-column-names (table-drop df '(age))) '(name gender))

  ; check head, tail
  (check-equal? (sequence->list (table-column (table-head df 2) 'name)) '("Jeff" "Jennie"))
  (check-equal? (sequence->list (table-column (table-tail df 2) 'name)) '("Dave" "Bob"))

  ; helper
  (define (age-filter age) (< age 30))

  ; check mapping, filtering, etc.
  (check-equal? (sequence->list (table-apply age-filter df '(age))) '(#f #f #t #t #f))
  (check-equal? (table-index (table-filter age-filter df '(age))) #(2 3))
  (check-equal? (table-index (table-sort df '(age))) #(2 3 4 1 0))
  (check-equal? (table-index (table-reverse df)) #(4 3 2 1 0))

  ; distinct column values
  (check-equal? (table-index (table-distinct df '(gender) 'first)) #(0 1))
  (check-equal? (table-index (table-distinct df '(gender) 'last)) #(2 4))
  (check-equal? (table-index (table-distinct df '(gender) 'none)) #())

  ; check reindexing
  (let ([rdf (table-reindex (table-filter age-filter df '(age)))])
    (check-equal? (table-index rdf) #(0 1))
    (check-equal? (table-data rdf) '((name . #("Isabel" "Dave"))
                                     (age . #(12 24))
                                     (gender . #(f m)))))

  ; check sorting of filtered data
  (let ([rdf (table-sort (table-filter age-filter df '(age)) '(age) sort-descending)])
    (check-equal? (table-index rdf) #(3 2)))

  ; check column updating
  (let ([rdf (table-with-column df (table-apply (λ (age) (- age 10)) df '(age)) #:as 'age)])
    (check-equal? (table-column-names rdf) '(name age gender))
    (check-equal? (column-data (table-column rdf 'age))
                  #(34 29 2 14 28)))

  ; check adding columns
  (let ([ndf (table-with-column empty-table (in-range 5))])
    (check-equal? (table-length ndf) 5)
    (check-true (all? (table-apply = ndf))))
  (let ([ndf (table-with-column df (table-apply age-filter df '(age)) #:as 'child)])
    (check-equal? (column-data (table-column ndf 'child)) #(#f #f #t #t #f))))
