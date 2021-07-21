#lang racket

(provide (all-defined-out)
         (except-out (struct-out index-stream)))

;; ----------------------------------------------------

(struct index-stream [ix data n]
  #:methods gen:stream
  [(define (stream-empty? s)
     (>= (index-stream-n s) (vector-length (index-stream-ix s))))

   ; get the key for this index
   (define (stream-first s)
     (let ([i (vector-ref (index-stream-ix s) (index-stream-n s))])
       (vector-ref (index-stream-data s) i)))

   ; advance to the next index
   (define (stream-rest s)
     (struct-copy index-stream s [n (add1 (index-stream-n s))]))])

;; ----------------------------------------------------

(define (index->stream ix data)
  (index-stream ix data 0))

;; ----------------------------------------------------

(define empty-index #())

;; ----------------------------------------------------

(define (build-index n)
  (build-vector n identity))

;; ----------------------------------------------------

(define index-length vector-length)

;; ----------------------------------------------------

(define index-empty? vector-empty?)

;; ----------------------------------------------------

(define (index-ref ix v n)
  (vector-ref v (vector-ref ix n)))

;; ----------------------------------------------------

(define (index-for-each proc ix v)
  (for ([i ix])
    (proc (vector-ref v i))))

;; ----------------------------------------------------

(define (index-map proc ix v)
  (vector-map (λ (i) (proc (vector-ref v i))) ix))

;; ----------------------------------------------------

(define (index-filter proc ix v)
  (vector-filter (λ (i) (proc (vector-ref v i))) ix))

;; ----------------------------------------------------

(define (index-head ix n)
  (vector-take ix (min (index-length ix) n)))

;; ----------------------------------------------------

(define (index-tail ix n)
  (vector-take-right ix (min (index-length ix) n)))

;; ----------------------------------------------------

(define (index-reverse ix)
  (let ([n (index-length ix)])
    (build-vector n (λ (i) (vector-ref ix (- n i 1))))))

;; ----------------------------------------------------

(define (index-sort ix v less-than?)
  (vector-sort ix less-than? #:key (λ (n) (index-ref ix v n))))
