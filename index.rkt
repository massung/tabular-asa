#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

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

(define (index-compact ix v)
  (for/vector ([i ix])
    (vector-ref v i)))

;; ----------------------------------------------------

(define (index-ref ix v n)
  (vector-ref v (vector-ref ix n)))

;; ----------------------------------------------------

(define (index-for-each proc ix v)
  (for ([i ix])
    (proc i (vector-ref v i))))

;; ----------------------------------------------------

(define (index-map proc ix v)
  (vector-map (λ (i) (proc i (vector-ref v i))) ix))

;; ----------------------------------------------------

(define (index-filter proc ix v)
  (vector-filter (λ (i) (proc i (vector-ref v i))) ix))

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
  (vector-sort ix less-than? #:key (λ (n) (vector-ref v n))))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; build a simple indexes
  (define ix (build-index 5))
  (define ix-head (index-head ix 2))
  (define ix-tail (index-tail ix 2))
  (define ix-reverse (index-reverse ix))

  ; simple index tests
  (check-equal? ix #(0 1 2 3 4))
  (check-equal? ix-head #(0 1))
  (check-equal? ix-tail #(3 4))
  (check-equal? ix-reverse #(4 3 2 1 0))

  ; create some simple data to work with
  (define data #(a b c d e))

  ; test streams
  (check-equal? (stream->list (index->stream ix data)) '(a b c d e))
  (check-equal? (stream->list (index->stream ix-head data)) '(a b))
  (check-equal? (stream->list (index->stream ix-tail data)) '(d e))
  (check-equal? (stream->list (index->stream ix-reverse data)) '(e d c b a))

  ; test map + filter
  (check-equal? (index-map (λ (i x) (~a x)) ix data) #("a" "b" "c" "d" "e"))
  (check-equal? (index-filter (λ (i x) (even? i)) ix data) #(0 2 4)))
