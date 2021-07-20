#lang racket

(require racket/struct)

;; ----------------------------------------------------

(require "index.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define index-column (make-parameter '||))

;; ----------------------------------------------------

(struct table [index columns]
  #:property prop:sequence
  (λ (df) (table->row-stream df))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (df port mode)
       (fprintf port "#<table [~a rows x ~a cols]>"
                (sequence-length (table-index df))
                (sequence-length (table-columns df)))))])

;; ----------------------------------------------------

(define (table->row-stream df #:keep-index? [keep-index #t])
  (let* ([i (table-index df)]

         ; create a list of streams - one per column
         [ss (for/list ([col (table-columns df)])
               (index->stream (struct-copy index i [keys (second col)])))])

    ; zip the streams together into a single stream
    (stream-zip (if keep-index (cons (index->stream i) ss) ss))))

;; ----------------------------------------------------

(define (table->record-stream df #:keep-index? [keep-index #t])
  (let* ([columns (let ([names (table-column-names df)])
                    (if keep-index (cons (index-column) names) names))]

         ; convert each row (list) to a record (hash)
         [row->record (λ (row)
                        (for/hash ([k columns] [v row])
                          (values k v)))])
    (stream-map row->record (table->row-stream df #:keep-index? keep-index))))

;; ----------------------------------------------------

(define empty-table (table empty-index '()))

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
  (values (sequence-length (table-index df))
          (sequence-length (table-columns df))))

;; ----------------------------------------------------

(define (table-length df)
  (index-length (table-index df)))

;; ----------------------------------------------------

(define (table-empty? df)
  (or (zero? (table-length df))
      (empty? (table-columns df))))

;; ----------------------------------------------------

(define (table-column-names df)
  (map car (table-columns df)))

;; ----------------------------------------------------

(define (table-column df k)
  (table-index (table-index-by df k)))

;; ----------------------------------------------------

(define (table-row df n #:keep-index? [keep-index #t])
  (let* ([i (sequence-ref (table-index df) n)]
         [row (for/list ([col (table-columns df)])
                (vector-ref (second col) i))])
    (if keep-index (cons (index-ref (table-index df) n) row) row)))

;; ----------------------------------------------------

(define (table-record df i #:keep-index? [keep-index #t])
  (let ([columns (let ([names (table-column-names df)])
                   (if keep-index (cons #t names) names))])
    (for/hash ([k columns] [v (table-row df i)])
      (values k v))))

;; ----------------------------------------------------

(define (table-clear df)
  (struct-copy table df [index empty-index]))

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
               [columns (filter-not (λ (col)
                                      (member (car col) ks))
                                    (table-columns df))]))

;; ----------------------------------------------------

(define (table-drop-na df [ks (table-column-names df)])
  (let ([all (λ (x . xs)
               (for/and ([x (cons x xs)]) x))])
    (table-filter df (table-map all df ks #:drop-na? #t))))

;; ----------------------------------------------------

(define (table-cut df ks)
  (let ([cols (table-columns df)])
    (struct-copy table
                 df
                 [columns (filter identity (map (λ (name)
                                                  (assoc name cols))
                                                ks))])))

;; ----------------------------------------------------

(define (table-filter df seq)
  (let ([index (table-index df)])
    (struct-copy table
                 df
                 [index (for/vector ([i index] [f seq] #:when f) i)])))

;; ----------------------------------------------------

(define (table-sort-by df k less-than?)
  (let ([sorted-index (index-sort (table-column df k) less-than?)])
    (struct-copy table
                 df
                 [index (struct-copy index
                                     (table-index df)
                                     [ix (index-ix sorted-index)])])))

;; ----------------------------------------------------

(define (table-index-by df k #:drop-column? [drop #t])
  (let ([col (findf (λ (col)
                      (eq? k (car col)))
                    (table-columns df))])
    (struct-copy table
                 (if drop (table-drop df (list k)) df)
                 [index (struct-copy index
                                     (table-index df)
                                     [keys (second col)])])))

;; ----------------------------------------------------

(define (table-reverse df)
  (struct-copy table
               df
               [index (index-reverse (table-index df))]))
#|
;; ----------------------------------------------------

(define (table-with-column df seq #:name [name #f])
  (let ([name (or name (gensym "col"))])
    (if (empty? (table-columns df))
        (let* ([v (for/vector ([x seq]) x)]
               
               ; build the initial index + keys
               [i (build-index (vector-length v))])
          (table i (list (list name v))))

        ; use the existing index to build the new column
        (let* ([v (make-vector (table-length df) #f)]
               
               ; create the new column
               [col (list name v)])
          
          ; copy the values into the new column vector
          (for ([x seq] [i (table-index df)])
            (vector-set! v i x))
          
          ; append the new column of values
          (struct-copy table
                       df
                       [columns (append (table-columns df) (list col))])))))

;; ----------------------------------------------------

(define (table-with-columns-renamed df rename-map)
  (let ([columns (for/list ([col (table-columns df)])
                   (match col
                     [(list name v)
                      (let ([mapping (assoc name rename-map)])
                        (list (or (and mapping (second mapping)) name) v))]))])
    (struct-copy table df [columns columns])))
|#
