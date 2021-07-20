#lang racket

(provide (all-defined-out)
         (except-out (struct-out index-stream)))

;; ----------------------------------------------------

(struct index [ix keys]
  #:property prop:sequence
  (λ (index) (index-ix index))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (index port mode)
       (fprintf port "#<key-index [~a values]>"
                (sequence-length (index-ix index)))))])

;; ----------------------------------------------------

(struct index-stream [index n]
  #:methods gen:stream
  [(define (stream-empty? s)
     (let ([ix (index-ix (index-stream-index s))])
       (>= (index-stream-n s) (vector-length ix))))

   ; get the key for this index
   (define (stream-first s)
     (index-ref (index-stream-index s) (index-stream-n s)))

   ; advance to the next index
   (define (stream-rest s)
     (struct-copy index-stream s [n (add1 (index-stream-n s))]))])

;; ----------------------------------------------------

(define (index->stream i)
  (index-stream i 0))

;; ----------------------------------------------------

(define empty-index (index #() #()))

;; ----------------------------------------------------

(define (build-index n)
  (let ([ix (build-vector n identity)])
    (index ix ix)))

;; ----------------------------------------------------

(define (index-length i)
  (vector-length (index-ix i)))

;; ----------------------------------------------------

(define (index-ref i n)
  (vector-ref (index-keys i) (vector-ref (index-ix i) n)))

;; ----------------------------------------------------

(define (index-map proc i)
  (let ([v (make-vector (index-length i) #f)])
    (for ([n (in-naturals)]
          [x (index->stream i)])
      (vector-set! v n (proc x)))

    ; create a new index with different keys
    (struct-copy index
                 i
                 [keys v])))

;; ----------------------------------------------------

(define (index-head i n)
  (struct-copy index
               i
               [ix (vector-take (index-ix i)
                                (min (index-length i) n))]))

;; ----------------------------------------------------

(define (index-tail i n)
  (struct-copy index
               i
               [ix (vector-take-right (index-ix i)
                                      (min (index-length i) n))]))

;; ----------------------------------------------------

(define (index-reverse i)
  (let* ([v (index-ix i)]
         [n (vector-length v)])
    (struct-copy index
                 i
                 [ix (build-vector n (λ (i) (vector-ref v (- n i 1))))])))

;; ----------------------------------------------------

(define (index-sort i less-than?)
  (let ([key (λ (n) (index-ref i n))])
    (struct-copy index
                 i
                 [ix (vector-sort (index-ix i) less-than? #:key key)])))
