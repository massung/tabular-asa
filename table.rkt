#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")
(require "column.rkt")
(require "group.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct table [pk column-data indexes]
  #:property prop:sequence
  (λ (df) (table->row-stream df))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (df port mode)
       (fprintf port "#<table [~a rows x ~a cols]>"
                (column-length (table-pk df))
                (sequence-length (table-column-data df)))))])

;; ----------------------------------------------------

(define (table->row-stream df [index #f] #:keep-index? [keep-index #t])
  (let ([key-stream (column->stream (table-pk df))])
    (stream-map (λ (i)
                  (table-row df i #:keep-index? keep-index))
                (sequence->stream (if index
                                      index
                                      (table-index df))))))

;; ----------------------------------------------------

(define (table->record-stream df [index #f] #:keep-index? [keep-index #t])
  (let* ([columns (table-column-names df)]

         ; optionally prepend the index column name
         [ks (if keep-index (cons (column-name (table-pk df)) columns) columns)]

         ; convert each row (list) to a record (hash)
         [row->record (λ (row)
                        (for/hash ([k ks] [v row])
                          (values k v)))])
    (stream-map row->record (table->row-stream df index #:keep-index? keep-index))))

;; ----------------------------------------------------

(define empty-table (table (column null #() #()) '() #hash()))

;; ----------------------------------------------------

(define (table-length df)
  (column-length (table-pk df)))

;; ----------------------------------------------------

(define (table-shape df)
  (values (table-length df) (sequence-length (table-columns df))))

;; ----------------------------------------------------

(define (table-empty? df)
  (or (zero? (table-length df))
      (empty? (table-column-data df))))

;; ----------------------------------------------------

(define (table-index df [k #f] [less-than? #f])
  (if k
      (hash-ref! (table-indexes df)
                 (list k less-than?)
                 (λ ()
                   (let ([col (table-column df k)])
                     (build-secondary-index col less-than?))))
      (column-index (table-pk df))))

;; ----------------------------------------------------

(define (table-compact df #:reindex? [reindex #t])
  (let ([ix (table-index df)])
    (table (if reindex
               (build-index-column (index-length ix))
               (column-compact (table-pk df)))
           (for/list ([col (table-column-data df)])
             (match col
               [(cons name data)
                (cons name (index-compact ix data))]))
           (make-hash))))

;; ----------------------------------------------------

(define (table-for-each proc df [cut #f] #:keep-index? [keep-index #t])
  (stream-for-each (λ (row) (apply proc row))
                   (let ([df-cut (if cut (table-cut df cut) df)])
                     (table->row-stream df-cut #:keep-index? keep-index))))

;; ----------------------------------------------------

(define (table-map proc df [cut #f] #:keep-index? [keep-index #t])
  (let* ([df-cut (if cut (table-cut df cut) df)]
         [rows (table->row-stream df-cut #:keep-index? keep-index)])
    (stream-map (λ (row) (apply proc row)) rows)))

;; ----------------------------------------------------

(define (table-filter proc df [cut #f] #:keep-index? [keep-index #t])
  (let ([pk (table-pk df)])
    (struct-copy table
                 df
                 [pk (struct-copy column
                                  pk
                                  [index (for/vector ([i (column-index pk)]
                                                      [f (table-map proc
                                                                    df
                                                                    cut
                                                                    #:keep-index? keep-index)]
                                                      #:when f)
                                           i)])])))

;; ----------------------------------------------------

(define (table-column-names df)
  (map car (table-column-data df)))

;; ----------------------------------------------------

(define (table-columns df)
  (for/list ([col (table-column-data df)])
    (match col
      [(cons name data)
       (column name (table-index df) data)])))

;; ----------------------------------------------------

(define (table-column df k)
  (match (assq k (table-column-data df))
    [(cons name data)
     (column name (table-index df) data)]
    [_ (error "Column not found in table:" k)]))

;; ----------------------------------------------------

(define (table-row df n #:keep-index? [keep-index #t])
  (let* ([ix (table-index df)]
         [row (for/list ([col (table-column-data df)])
                (index-ref ix (cdr col) n))])
    (if keep-index (cons (column-ref (table-pk df) n) row) row)))

;; ----------------------------------------------------

(define (table-record df n #:keep-index? [keep-index #t])
  (let ([cols (table-columns df)])
    (for/hash ([col (if keep-index (cons (table-pk df) cols) cols)])
      (values (column-name col) (column-ref col n)))))

;; ----------------------------------------------------

(define (table-head df [n 10])
  (struct-copy table
               df
               [pk (column-head (table-pk df) n)]))

;; ----------------------------------------------------

(define (table-tail df [n 10])
  (struct-copy table
               df
               [pk (column-tail (table-pk df) n)]))

;; ----------------------------------------------------

(define (table-drop-na df [ks (table-column-names df)])
  (table-filter df (table-map all? (table-cut df ks))))

;; ----------------------------------------------------

(define (table-drop df ks)
  (struct-copy table
               df
               [column-data (filter-not (λ (col)
                                          (member (car col) ks))
                                        (table-column-data df))]))

;; ----------------------------------------------------

(define (table-cut df ks)
  (let ([cols (table-column-data df)])
    (struct-copy table
                 df
                 [column-data (filter identity
                                      (for/list ([k ks])
                                        (match k
                                          [(list org as)
                                           (cons as (cdr (assq org cols)))]
                                          [else
                                           (assq k cols)])))])))

;; ----------------------------------------------------

(define (table-sort-by df k less-than?)
  (let ([pk (table-pk df)])
    (struct-copy table
                 df
                 [pk (let ([sorted (column-sort (table-column df k) less-than?)])
                       (struct-copy column
                                    pk
                                    [index (column-index sorted)]))])))

;; ----------------------------------------------------

(define (table-index-by df k #:drop? [drop #t])
  (struct-copy table
               (if drop (table-drop df (list k)) df)
               [pk (table-column df k)]))

;; ----------------------------------------------------

(define (table-with-index df seq #:name [name '||])
  (let ([pk (table-pk df)])
    (struct-copy table
                 df
                 [pk (let ([ix (for/vector ([i seq]) i)])
                       (column name ix (column-data pk)))])))

;; ----------------------------------------------------

(define (table-distinct df k [keep 'first])
  (let ([col (table-column df k)])
    (struct-copy table
                 df
                 [pk (let ([ix (build-secondary-index col #f keep)])
                       (struct-copy column
                                    (table-pk df)
                                    [index (secondary-index->index ix)]))])))

;; ----------------------------------------------------

(define (table-reverse df)
  (struct-copy table
               df
               [pk (column-reverse (table-pk df))]))
