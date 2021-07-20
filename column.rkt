#lang racket

(provide (all-defined-out)
         (except-out (struct-out column-stream)))

;; ----------------------------------------------------

(struct column [index keys]
  #:property prop:sequence
  (λ (col) (column-index col))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (col port mode)
       (fprintf port "#<column [~a values]>"
                (sequence-length (column-index col)))))])

;; ----------------------------------------------------

(struct column-stream [col n]
  #:methods gen:stream
  [(define (stream-empty? s)
     (>= (column-stream-n s) (column-length (column-stream-col s))))

   ; get the key for this index
   (define (stream-first s)
     (column-ref (column-stream-col s) (column-stream-n s)))

   ; advance to the next index
   (define (stream-rest s)
     (struct-copy column-stream s [n (add1 (column-stream-n s))]))])

;; ----------------------------------------------------

(define (column->stream col)
  (column-stream col 0))

;; ----------------------------------------------------

(define empty-column (column #() #()))

;; ----------------------------------------------------

(define (make-column n)
  (let ([ix (build-vector n identity)])
    (column ix ix)))

;; ----------------------------------------------------

(define (column-length col)
  (vector-length (column-index col)))

;; ----------------------------------------------------

(define (column-ref col n)
  (vector-ref (column-keys col) (vector-ref (column-index col) n)))

;; ----------------------------------------------------

(define (column-map proc col)
  (let ([v (make-vector (column-length col) #f)])
    (for ([n (in-naturals)]
          [x (column->stream col)])
      (vector-set! v n (proc x)))

    ; create a new index with different keys
    (struct-copy column
                 col
                 [keys v])))

;; ----------------------------------------------------

(define (column-head col n)
  (struct-copy column
               col
               [index (vector-take (column-index col)
                                   (min (column-length col) n))]))

;; ----------------------------------------------------

(define (column-tail col n)
  (struct-copy column
               col
               [index (vector-take-right (column-index col)
                                         (min (column-length col) n))]))

;; ----------------------------------------------------

(define (column-reverse col)
  (let* ([v (column-index col)]
         [n (column-length col)])
    (struct-copy column
                 col
                 [index (build-vector n (λ (i) (vector-ref v (- n i 1))))])))

;; ----------------------------------------------------

(define (column-sort col less-than?)
  (let ([key (λ (n) (column-ref col n))])
    (struct-copy column
                 col
                 [index (vector-sort (column-index col) less-than? #:key key)])))
