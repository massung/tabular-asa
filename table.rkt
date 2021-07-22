#lang racket

(require "index.rkt")
(require "column.rkt")
(require "group.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct table [pk column-data]
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

(define (table->row-stream df #:keep-index? [keep-index #t])
  (let* ([key-stream (column->stream (table-pk df))]

         ; create a stream per column
         [cols (map column->stream (table-columns df))]

         ; optionally prepend the index
         [streams (if keep-index (cons key-stream cols) cols)])
    (stream-zip streams)))

;; ----------------------------------------------------

(define (table->record-stream df #:keep-index? [keep-index #t])
  (let* ([columns (table-column-names df)]

         ; optionally prepend the index column name
         [ks (if keep-index (cons (column-name (table-pk df)) columns) columns)]

         ; convert each row (list) to a record (hash)
         [row->record (λ (row)
                        (for/hash ([k ks] [v row])
                          (values k v)))])
    (stream-map row->record (table->row-stream df #:keep-index? keep-index))))

;; ----------------------------------------------------

(define empty-table (table (column null #() #()) '()))

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

(define (table-index df)
  (column-index (table-pk df)))

;; ----------------------------------------------------

(define (table-compact df #:reindex? [reindex #t])
  (let ([ix (table-index df)])
    (table (if reindex
               (build-index-column (index-length ix))
               (column-compact (table-pk df)))
           (for/list ([col (table-column-data df)])
             (match col
               [(cons name data)
                (cons name (index-compact ix data))])))))

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
  (let ([row (for/list ([col (table-columns df)])
               (column-ref col n))])
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
                                      (map (λ (name) (assq name cols)) ks))])))

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

;; ----------------------------------------------------

(define (table-with-column df seq #:name [name #f])
  (let ([k (or name (gensym "col"))])
    (if (table-empty? df)
        (let ([data (for/vector ([x seq]) x)])
          (table (build-index-column (vector-length data)) (list (cons k data))))
        
        ; match the pk size, insert values into new column
        (let* ([pk (table-pk df)]
               [ix (column-index pk)]
               [n (vector-length (column-data pk))]
               [data (make-vector n #f)]
               [col (list (cons k data))])
          (index-for-each (λ (i x) (vector-set! data i x)) ix seq)
          (struct-copy table
                       df
                       [column-data (append (table-column-data df) col)])))))
#|
;; ----------------------------------------------------

(define (table-with-columns-renamed df rename-assocs)
  (let ([columns (for/list ([col (table-columns df)])
                   (match col
                     [(list name v)
                      (let ([mapping (assq name rename-map)])
                        (list (or (and mapping (second mapping)) name) v))]))])
    (struct-copy table df [columns columns])))
|#
