#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require racket/generator)

;; ----------------------------------------------------

(require "compare.rkt")

;; ----------------------------------------------------

(provide (all-defined-out)
         (except-out (struct-out index-stream)))

;; ----------------------------------------------------

(struct index [keys less-than?]
  #:property prop:sequence
  (λ (ix) (index-scan ix))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (ix port mode)
       (fprintf port "#<index [~a keys]>" (index-length ix))))])

;; ----------------------------------------------------

(struct index-stream [g i]
  #:methods gen:stream
  [(define (stream-empty? s)
     (not (index-stream-i s)))
   (define (stream-first s)
     (index-stream-i s))
   (define (stream-rest s)
     (let ([g (index-stream-g s)])
       (index-stream g (g))))])

;; ----------------------------------------------------

(define (index-scan ix #:from [from #f] #:to [to #f])
  (let* ([n (index-length ix)]
         [keys (index-keys ix)]

         ; should the key lookup be exact?
         [exact? (and to from (equal? to from))]

         ; key range indices of the scan
         [start (if from (index-find ix from exact?) 0)]
         [end (or (and exact? start)
                  (if to (index-find ix to #f) n))]

         ; the final range
         [key-range (cond
                      [(or (not start) (>= start n))
                       empty-sequence]

                      ; only the start key is scanned
                      [(= start end)
                       (in-range start (add1 start))]

                      ; exclusive to
                      [else
                       (in-range start end)])]

         ; build a generator for iteration
         [g (generator ()
              (for ([k key-range])
                (let ([indices (cdr (vector-ref keys k))])
                  (for ([i indices])
                    (yield i))))

              ; end of key range...
              (for ([i (in-cycle '(#f))])
                (yield i)))])
    (index-stream g (g))))

;; ----------------------------------------------------

(define (build-index seq [less-than? less-than?])
  (let ([h (make-hash)])
    (for ([(x i) (in-indexed seq)] #:when x)
      (hash-update! h x (λ (ix) (cons i ix)) '()))

    ; build the key-space vector, then sort the keys
    (let ([keys (for/vector ([(k indices) h])
                  (cons k (reverse indices)))])
      (vector-sort! keys less-than? #:key car)

      ; build the final index
      (index keys less-than?))))

;; ----------------------------------------------------

(define empty-index (index #() <))

;; ----------------------------------------------------

(define (index-length ix)
  (vector-length (index-keys ix)))

;; ----------------------------------------------------

(define (index-empty? ix)
  (zero? (index-length ix)))

;; ----------------------------------------------------

(define (index-find ix key [exact #t])
  (let* ([keys (index-keys ix)]
         [n (vector-length keys)]
         [less-than? (index-less-than? ix)])
    (letrec ([search (λ (i start end)
                       (let ([group (vector-ref keys i)])
                         (cond
                           [(equal? key (car group)) i]

                           ; nothing more to search?
                           [(= i start)
                            (and (not exact)
                                 (if (less-than? key (car group))
                                     i
                                     end))]

                           ; search left or right?
                           [else
                            (if (less-than? key (car group))
                                (let ([ni (arithmetic-shift (+ start i) -1)])
                                  (search ni start i))
                                (let ([ni (arithmetic-shift (+ i end) -1)])
                                  (search ni i end)))])))])
      (if (index-empty? ix)
          #f
          (search (arithmetic-shift n -1) 0 n)))))

;; ----------------------------------------------------

(define (index-member ix key)
  (let ([i (index-find ix key)])
    (and i (index-ref ix i))))

;; ----------------------------------------------------

(define (index-ref ix n)
  (vector-ref (index-keys ix) n))

;; ----------------------------------------------------

(define (index-map ix v #:from [from #f] #:to [to #f])
  (sequence-map (λ (i) (vector-ref v i)) (index-scan ix #:from from #:to to)))

;; ----------------------------------------------------

(define (index-min ix)
  (if (index-empty? ix)
      #f
      (index-ref ix 0)))

;; ----------------------------------------------------

(define (index-max ix)
  (let ([n (index-length ix)])
    (if (zero? n)
        #f
        (index-ref ix (sub1 n)))))

;; ----------------------------------------------------

(define (index-median ix)
  (if (index-empty? ix)
      #f
      (let ([n (length (cdr (index-min ix)))])
        (for/fold ([i 0]
                   [m n]
                   [t n]
                   #:result (index-ref ix i))
                  ([key (sequence-tail (index-keys ix) 1)])
          (let ([count (+ (length (cdr key)) t)])
            (if (>= count (* m 2))
                (values (+ i 1) (+ m (length (cdr (index-ref ix i)))) count)
                (values i m count)))))))

;; ----------------------------------------------------

(define (index-mode ix)
  (if (index-empty? ix)
      #f
      (vector-argmax length (index-keys ix))))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; build a simple sequence of integers and index them
  (define xs #(6 3 6 0 4 1 0 9 9 9 4 9 7 1 9 3 5 4 5 8 8 5 3 1 4 5 6 8 0 6))
  (define sorted (vector-sort xs <))
  (define ix (build-index xs))

  ; ensure the order and integrity of the index
  (check-equal? (sequence->list (sequence-map (λ (i) (vector-ref xs i)) ix))
                (vector->list sorted))

  ; check min, max, median, and mode
  (check-equal? (car (index-min ix)) (vector-argmin identity xs))
  (check-equal? (car (index-max ix)) (vector-argmax identity xs))
  (check-equal? (car (index-median ix)) (vector-ref sorted (quotient (vector-length xs) 2)))
  (check-equal? (car (index-mode ix)) (argmax (λ (n)
                                                (vector-count (λ (x) (= x n)) xs))
                                              (range 10)))

  ; ensure keys not found
  (check-false (index-find ix -1))

  ; check scanning and mapping
  (check-equal? (sequence->list (index-map ix xs #:from 100)) '())
  (check-equal? (sequence->list (index-map ix xs #:to 4)) '(0 0 0 1 1 1 3 3 3))
  (check-equal? (sequence->list (index-map ix xs #:from 6)) '(6 6 6 6 7 8 8 8 9 9 9 9 9))
  (check-equal? (sequence->list (index-map ix xs #:from 2 #:to 6)) '(3 3 3 4 4 4 4 5 5 5 5))
  (check-equal? (sequence->list (index-map ix xs #:from 5 #:to 5)) '(5 5 5 5))
  (check-equal? (sequence->list (index-map ix xs #:from 2 #:to 2)) '()))
