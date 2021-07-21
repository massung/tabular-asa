#lang racket

(require racket/struct)

;; ----------------------------------------------------

(require "index.rkt")
(require "column.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define index-column (make-parameter '||))

;; ----------------------------------------------------

(struct table [index keys column-data]
  #:property prop:sequence
  (λ (df) (table->row-stream df))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (df port mode)
       (fprintf port "#<table [~a rows x ~a cols]>"
                (index-length (table-index df))
                (sequence-length (table-column-data df)))))])

;; ----------------------------------------------------

(define (table->row-stream df #:keep-index? [keep-index #t])
  (let* ([key-stream (column->stream (table-key-column df))]

         ; create a stream per column
         [cols (map column->stream (table-columns df))]

         ; optionally prepend the index
         [streams (if keep-index (cons key-stream cols) cols)])
    (stream-zip streams)))

;; ----------------------------------------------------

(define (table->record-stream df #:keep-index? [keep-index #t])
  (let* ([columns (table-column-names df)]

         ; optionally prepend the index column name
         [ks (if keep-index (cons (index-column) columns) columns)]

         ; convert each row (list) to a record (hash)
         [row->record (λ (row)
                        (for/hash ([k ks] [v row])
                          (values k v)))])
    (stream-map row->record (table->row-stream df #:keep-index? keep-index))))

;; ----------------------------------------------------

(define empty-table (table empty-index #() '()))

;; ----------------------------------------------------

(define (table-for-each proc df #:keep-index? [keep-index #t])
  (stream-for-each proc (table->row-stream df #:keep-index? keep-index)))

;; ----------------------------------------------------

(define (table-for-each-apply proc df #:keep-index? [keep-index #t])
  (table-for-each (λ (row) (apply proc row)) df #:keep-index? keep-index))

;; ----------------------------------------------------

(define (table-map proc df #:keep-index? [keep-index #t] #:drop-na? [drop-na #f])
  (let ([rows (table->row-stream df #:keep-index? keep-index)])
    (stream-map (λ (row) (apply proc row))
                (if (not drop-na)
                    rows
                    (stream-filter (λ (row) (for/and ([x row]) x)) rows)))))

;; ----------------------------------------------------

(define (table-shape df)
  (values (column-length (table-index df))
          (sequence-length (table-columns df))))

;; ----------------------------------------------------

(define (table-length df)
  (index-length (table-index df)))

;; ----------------------------------------------------

(define (table-empty? df)
  (or (zero? (table-length df))
      (empty? (table-column-data df))))

;; ----------------------------------------------------

(define (table-key-column df)
  (column (index-column) (table-index df) (table-keys df)))

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

         ; get the values of each column
         [row (for/list ([col (table-columns df)])
                (column-ref col n))])

    ; optionally prepend the index value
    (if keep-index (cons (index-ref ix (table-keys df) n) row) row)))

;; ----------------------------------------------------

(define (table-record df n #:keep-index? [keep-index #t])
  (let ([cols (table-columns df)])
    (for/hash ([col (if keep-index (cons (table-key-column df) cols) cols)])
      (values (column-name col) (column-ref col n)))))

;; ----------------------------------------------------

(define (table-head df [n 10])
  (struct-copy table
               df
               [index (index-head (table-index df) n)]))

;; ----------------------------------------------------

(define (table-tail df [n 10])
  (struct-copy table
               df
               [index (index-tail (table-index df) n)]))

;; ----------------------------------------------------

(define (table-drop df ks)
  (struct-copy table
               df
               [column-data (filter-not (λ (col)
                                          (member (car col) ks))
                                        (table-column-data df))]))

;; ----------------------------------------------------

(define (table-drop-na df [ks (table-column-names df)])
  (let ([all (λ (x . xs)
               (for/and ([x (cons x xs)]) x))])
    (table-filter df (table-map all df ks #:drop-na? #t))))

;; ----------------------------------------------------

(define (table-cut df ks)
  (let ([cols (table-column-data df)])
    (struct-copy table
                 df
                 [column-data (filter identity
                                      (map (λ (name) (assq name cols)) ks))])))

;; ----------------------------------------------------

(define (table-filter df seq)
  (let ([index (table-index df)])
    (struct-copy table
                 df
                 [index (for/vector ([i index] [f seq] #:when f) i)])))

;; ----------------------------------------------------

(define (table-sort-by df k less-than?)
  (let ([v (cdr (assq k (table-column-data df)))])
    (struct-copy table
                 df
                 [index (index-sort (table-index df) v less-than?)])))

;; ----------------------------------------------------

(define (table-index-by df k #:drop-column? [drop #t])
  (let ([col (table-column df k)])
    (struct-copy table
                 (if drop (table-drop df (list k)) df)
                 [keys (column-data col)])))

;; ----------------------------------------------------

(define (table-reverse df)
  (struct-copy table
               df
               [index (index-reverse (table-index df))]))

;; ----------------------------------------------------

(define (table-with-column df seq #:name [name #f])
  (let ([k (or name (gensym "col"))])
    (if (table-empty? df)
        (let* ([data (for/vector ([x seq]) x)]
             
               ; dummy index
               [ix (build-index (vector-length data))])
          (table ix ix (list (cons k data))))

        ; match the key-column size, insert values into new column
        (let ([data (make-vector (vector-length (table-keys df)) #f)])
          (for ([i (table-index df)] [x seq])
            (vector-set! data i x))
          (struct-copy table
                       df
                       [column-data (let ([col (list (cons k data))])
                                      (append (table-column-data df) col))])))))
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
