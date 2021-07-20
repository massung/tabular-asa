#lang racket

(require racket/struct)

;; ----------------------------------------------------

(provide (all-defined-out)
         (except-out (struct-out index-stream))
         (except-out (struct-out row-stream)))

;; ----------------------------------------------------

(define index-column (make-parameter '||))

;; ----------------------------------------------------

(struct table [index keys columns]
  #:property prop:sequence
  (λ (df) (row-stream df 0 #t))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (df port mode)
       (fprintf port "#<table [~a rows x ~a cols]>"
                (sequence-length (table-index df))
                (sequence-length (table-columns df)))))])

;; ----------------------------------------------------

(struct index-stream [i index keys]
  #:methods gen:stream
  [(define (stream-empty? s)
     (>= (index-stream-i s) (vector-length (index-stream-index s))))

   ; get the key for this index
   (define (stream-first s)
     (let ([n (vector-ref (index-stream-index s) (index-stream-i s))])
       (vector-ref (index-stream-keys s) n)))

   ; advance to the next index
   (define (stream-rest s)
     (struct-copy index-stream s [i (add1 (index-stream-i s))]))])

;; ----------------------------------------------------

(struct row-stream [df i keep-index?]
  #:methods gen:stream
  [(define (stream-empty? s)
     (>= (row-stream-i s) (table-length (row-stream-df s))))

   ; get the row as a list of values at this index
   (define (stream-first s)
     (let* ([df (row-stream-df s)]
            [i (row-stream-i s)]
            [keys (table-keys df)]
            [ix (vector-ref (table-index df) i)]
            [row (for/list ([col (table-columns df)])
                   (vector-ref (second col) ix))])
       (if (row-stream-keep-index? s) (cons (vector-ref keys ix) row) row)))

   ; advance to the next index
   (define (stream-rest s)
     (struct-copy row-stream s [i (add1 (row-stream-i s))]))])

;; ----------------------------------------------------

(define (row-stream->record-stream s)
  (let* ([columns (let ([names (table-column-names (row-stream-df s))])
                    (if (row-stream-keep-index? s) (cons (index-column) names) names))]

         ; convert each row (a list) to a record (a hash)
         [row->record (λ (row)
                        (for/hash ([k columns] [v row])
                          (values k v)))])
    (stream-map row->record s)))

;; ----------------------------------------------------

(define (table->row-stream df #:keep-index? [keep-index #t])
  (row-stream df 0 keep-index))

;; ----------------------------------------------------

(define (table->record-stream df #:keep-index? [keep-index #t])
  (row-stream->record-stream (table->row-stream df #:keep-index? keep-index)))

;; ----------------------------------------------------

(define (key-stream df)
  (index-stream 0 (table-index df) (table-keys df)))

;; ----------------------------------------------------

(define (column-stream df k)
  (let ([col (findf (λ (col) (eq? k (car col))) (table-columns df))])
    (and col (index-stream 0 (table-index df) (second col)))))

;; ----------------------------------------------------

(define empty-table (table #() #() '()))

;; ----------------------------------------------------

(define (table-for-each proc df #:keep-index? [keep-index #t])
  (stream-for-each proc (table->row-stream df #:keep-index? keep-index)))

;; ----------------------------------------------------

(define (table-for-each-apply proc df #:keep-index? [keep-index #t])
  (table-for-each (λ (row) (apply proc row)) df #:keep-index? keep-index))

;; ----------------------------------------------------

(define (table-map proc df [ks (table-column-names df)] #:drop-na? [drop-na #f])
  (let ([columns (for/list ([k ks])
                   (table-column df k))])
    (for/stream ([i (table-index df)])
      (let ([xs (map (λ (v) (vector-ref v i)) columns)])
        (and (or (not drop-na)
                 (for/and ([x xs]) x))
             (apply proc xs))))))

;; ----------------------------------------------------

(define (table-map-index proc df [ks (table-column-names df)] #:drop-na? [drop-na #f])
  (let ([columns (cons (table-keys df) (for/list ([k ks])
                                         (table-column df k)))])
    (for/stream ([i (table-index df)])
      (let ([xs (map (λ (v) (vector-ref v i)) columns)])
        (and (or (not drop-na)
                 (for/and ([x xs]) x))
             (apply proc xs))))))

;; ----------------------------------------------------

(define (table-shape df)
  (values (sequence-length (table-index df))
          (sequence-length (table-columns df))))

;; ----------------------------------------------------

(define (table-length df)
  (vector-length (table-index df)))

;; ----------------------------------------------------

(define (table-empty? df)
  (or (zero? (table-length df))
      (empty? (table-columns df))))

;; ----------------------------------------------------

(define (table-column-names df)
  (map car (table-columns df)))

;; ----------------------------------------------------

(define (table-column df k)
  (let ([col (findf (λ (col) (eq? k (car col))) (table-columns df))])
    (and col (second col))))

;; ----------------------------------------------------

(define (table-row df i #:keep-index? [keep-index #t])
  (let* ([ix (vector-ref (table-index df) i)]
         [row (for/list ([col (table-columns df)])
                (vector-ref (second col) ix))])
    (if keep-index (cons (vector-ref  (table-keys df) ix) row) row)))

;; ----------------------------------------------------

(define (table-record df i #:keep-index? [keep-index #t])
  (let ([columns (let ([names (table-column-names df)])
                   (if keep-index (cons #t names) names))])
    (for/hash ([k columns] [v (table-row df i)])
      (values k v))))

;; ----------------------------------------------------

(define (table-clear df)
  (struct-copy table df [index #()]))

;; ----------------------------------------------------

(define (table-head df [n 10])
  (let* ([i (table-index df)]
         [n (min n (vector-length i))])
    (struct-copy table df [index (vector-take i n)])))

;; ----------------------------------------------------

(define (table-tail df [n 10])
  (let* ([i (table-index df)]
         [n (min n (vector-length i))])
    (struct-copy table df [index (vector-take-right i n)])))

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

(define (table-sort-by df column-name less-than?)
  (let* ([v (table-column df column-name)]
         [key (λ (i) (vector-ref v i))]
         [sorted (vector-sort (table-index df) less-than? #:key key)])
    (struct-copy table df [index sorted])))

;; ----------------------------------------------------

(define (table-index-by df column-name #:drop-column? [drop #t])
  (let ([keys (table-column df column-name)])
    (struct-copy table
                 (if drop (table-drop df column-name) df)
                 [keys keys])))

;; ----------------------------------------------------

(define (table-reverse df)
  (let* ([ix (table-index df)]
         [n (vector-length ix)])
    (struct-copy table df [index (build-vector n (λ (i)
                                                   (vector-ref ix (- n i 1))))])))

;; ----------------------------------------------------

(define (table-with-column df seq #:name [name #f])
  (let ([name (or name (gensym "col"))])
    (if (empty? (table-columns df))
        (let* ([v (for/vector ([x seq]) x)]
               
               ; build the initial index + keys
               [i (build-vector (vector-length v) identity)])
          (table i i (list (list name v))))

        ; use the existing index to build the new column
        (let* ([v (make-vector (vector-length (table-keys df)) #f)]
               
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
