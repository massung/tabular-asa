#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "index.rkt")
(require "column.rkt")

;; ----------------------------------------------------

(provide (all-defined-out)
         (except-out (struct-out secondary-index-stream)))

;; ----------------------------------------------------

(struct secondary-index [column keys less-than?]
  #:property prop:sequence
  (λ (ix) (secondary-index->stream ix))
  
  #:methods gen:custom-write
  [(define write-proc
     (λ (ix port mode)
       (fprintf port "#<secondary-index ~a [~a keys]>"
                (secondary-index-name ix)
                (secondary-index-length ix))))])

;; ----------------------------------------------------

(struct secondary-index-stream [i to keys xs]
  #:methods gen:stream
  [(define (stream-empty? s)
     (not (secondary-index-stream-xs s)))

   ; get the next index for this key
   (define (stream-first s)
     (car (secondary-index-stream-xs s)))

   ; advance to the next index and/or key
   (define (stream-rest s)
     (let ([xs (cdr (secondary-index-stream-xs s))])
       (if (null? xs)
           (let ([k (secondary-index-stream-keys s)]
                 [i (add1 (secondary-index-stream-i s))])
             (struct-copy secondary-index-stream
                          s
                          [i i]
                          [xs (if (>= i (secondary-index-stream-to s))
                                  #f
                                  (cdr (vector-ref k i)))]))
           (struct-copy secondary-index-stream
                        s
                        [xs xs]))))])
  
;; ----------------------------------------------------

(define (secondary-index->stream ix #:from [from #f] #:to [to #f])
  (let* ([n (secondary-index-length ix)]
         [keys (secondary-index-keys ix)]
         [start (if from (secondary-index-find ix from #f) 0)]
         [end (if to (secondary-index-find ix to #f) n)])
    (if (zero? n)
        (secondary-index-stream 0 0 #() #f)
        (let ([xs (secondary-index-ref ix start)])
          (cond
            [(> end start)
             (secondary-index-stream start end keys (cdr xs))]
            [(or (not from) (equal? from (car xs)))
             (secondary-index-stream start (add1 end) keys (cdr xs))]
            [else
             (secondary-index-stream 0 0 #() #f)])))))
  
;; ----------------------------------------------------

(define (secondary-index->index ix)
  (for/vector ([i ix]) i))

;; ----------------------------------------------------

(define (build-secondary-index column less-than? [keep 'all])
  (let* ([h (make-hash)]
         [insert (λ (i x)
                   (when x
                     (hash-update! h
                                   x
                                   (λ (ix)
                                     (case keep
                                       [(first) (if (null? ix) (list i) ix)]
                                       [(last)  (list i)]
                                       [(none)  (and (null? ix) (list i))]
                                       [else    (cons i ix)]))
                                   '())))])

    ; build the secondary index hash
    (column-for-each insert column)

    ; convert the hash into a vector of (key . indices)
    (let* ([keys (for/vector ([(k indices) h] #:when indices)
                   (cons k (reverse indices)))])

      ; sort the keys, but only if there's a compare function
      (when less-than?
        (vector-sort! keys less-than? #:key car))

      ; build the final, secondary index
      (secondary-index column keys less-than?))))

;; ----------------------------------------------------

(define (secondary-index-name ix)
  (column-name (secondary-index-column ix)))

;; ----------------------------------------------------

(define (secondary-index-length ix)
  (vector-length (secondary-index-keys ix)))

;; ----------------------------------------------------

(define (secondary-index-empty? ix)
  (zero? (secondary-index-length ix)))

;; ----------------------------------------------------

(define (secondary-index-sorted? ix)
  (not (not (secondary-index-less-than? ix))))

;; ----------------------------------------------------

(define (secondary-index-count ix [key #f])
  (if key
      (let ([ks (secondary-index-member ix key)])
        (if (not ks)
            0
            (length (cdr ks))))
      (for/sum ([key (secondary-index-keys ix)])
        (length (cdr key)))))

;; ----------------------------------------------------

(define (secondary-index-find ix key [exact #t])
  (let* ([keys (secondary-index-keys ix)]
         [n (vector-length keys)]
         [less-than? (secondary-index-less-than? ix)])
    (if (not less-than?)

        ; unsorted - just O(n) search the keyspace (must be exact)
        (for/first ([k keys] [i (range n)] #:when (equal? (car k) key)) i)

        ; sorted index, so it's possible to binary search
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
          (if (secondary-index-empty? ix)
              #f
              (search (arithmetic-shift n -1) 0 n))))))

;; ----------------------------------------------------

(define (secondary-index-member ix key)
  (let ([i (secondary-index-find ix key)])
    (and i (secondary-index-ref ix i))))

;; ----------------------------------------------------

(define (secondary-index-ref ix n)
  (vector-ref (secondary-index-keys ix) n))

;; ----------------------------------------------------

(define (secondary-index-min ix)
  (and (secondary-index-less-than? ix)
       (if (secondary-index-empty? ix)
           #f
           (secondary-index-ref ix 0))))

;; ----------------------------------------------------

(define (secondary-index-max ix)
  (and (secondary-index-less-than? ix)
       (let ([n (secondary-index-length ix)])
         (if (zero? n)
             #f
             (secondary-index-ref ix (sub1 n))))))

;; ----------------------------------------------------

(define (secondary-index-mean ix)
  (for/fold ([n 0] [m 0] #:result (/ n m))
            ([key (secondary-index-keys ix)])
    (match key
      [(list x xs ...)
       (let ([count (length xs)])
         (values (+ n (* x count)) (+ m count)))])))

;; ----------------------------------------------------

(define (secondary-index-median ix)
  (and (secondary-index-less-than? ix)
       (let ([n (secondary-index-length ix)])
         (if (zero? n)
             #f
             (secondary-index-ref ix (quotient n 2))))))

;; ----------------------------------------------------

(define (secondary-index-mode ix)
  (for/fold ([k #f] [n 0] #:result k)
            ([key (secondary-index-keys ix)])
    (let ([count (length key)])
      (if (<= count n)
          (values k n)
          (values key count)))))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; build a simple column in memory of names
  (define col (build-column '("Jeff"
                              "Jennie"
                              "Isabel"
                              "Dave"
                              "Bob"
                              "Patrick"
                              "Norah"
                              "Jeff"
                              "Dave"
                              "Faye"
                              "Marnie"
                              "Jeff"
                              "Isabel"
                              "Norah"
                              "Webber"
                              "Patrick")))

  ; build the index for the name and sort it
  (define ix (build-secondary-index col string<?))

  ; initial validations
  (check-equal? (secondary-index-length ix) 10)
  (check-equal? (secondary-index-count ix) 16)
  (check-equal? (secondary-index-count ix "Jeff") 3)
  (check-true (secondary-index-sorted? ix))

  ; lookup some keys
  (check-equal? (secondary-index-find ix "Bob") 0)
  (check-equal? (secondary-index-find ix "Jeff") 4)
  (check-equal? (secondary-index-find ix "Norah") 7)

  ; test index streams
  (let ([ix-stream (λ (from to)
                     (stream->list (secondary-index->stream ix #:from from #:to to)))])
    (check-equal? (ix-stream #f "Jeff") '(4 3 8 9 2 12))
    (check-equal? (ix-stream "Norah" #f) '(6 13 5 15 14))
    (check-equal? (ix-stream "Chuck" "Jeff") '(3 8 9 2 12))
    (check-equal? (ix-stream "Chuck" "Chuck") '())
    (check-equal? (ix-stream "Patrick" "Patrick") '(5 15)))

  ; refernces
  (check-equal? (secondary-index-ref ix 4) '("Jeff" 0 7 11))
  (check-equal? (secondary-index-ref ix 5) '("Jennie" 1))

  ; check failures
  (check-false (secondary-index-find ix "David"))
  (check-false (secondary-index-find ix "Andy"))
  (check-false (secondary-index-find ix "Xavier"))

  ; check non-exact failures
  (check-equal? (secondary-index-find ix "David" #f)
                (secondary-index-find ix "Faye"))
  (check-equal? (secondary-index-find ix "Andy" #f)
                (secondary-index-find ix "Bob"))
  (check-equal? (secondary-index-find ix "Xavier" #f)
                (secondary-index-length ix))

  ; member indices lookup
  (check-equal? (secondary-index-member ix "Jeff") '("Jeff" 0 7 11))
  (check-equal? (secondary-index-member ix "Jennie") '("Jennie" 1))
  (check-false (secondary-index-member ix "Xavier"))

  ; check median and mode
  (check-equal? (secondary-index-median ix) '("Jennie" 1))
  (check-equal? (secondary-index-mode ix) '("Jeff" 0 7 11))

  ; build distinct - take first
  (check-equal? (secondary-index-keys (build-secondary-index col string<? 'first))
                #(("Bob" 4)
                  ("Dave" 3)
                  ("Faye" 9)
                  ("Isabel" 2)
                  ("Jeff" 0)
                  ("Jennie" 1)
                  ("Marnie" 10)
                  ("Norah" 6)
                  ("Patrick" 5)
                  ("Webber" 14)))

  ; build distinct - take last
  (check-equal? (secondary-index-keys (build-secondary-index col string<? 'last))
                #(("Bob" 4)
                  ("Dave" 8)
                  ("Faye" 9)
                  ("Isabel" 12)
                  ("Jeff" 11)
                  ("Jennie" 1)
                  ("Marnie" 10)
                  ("Norah" 13)
                  ("Patrick" 15)
                  ("Webber" 14)))

  ; build distinct - drop all duplicates
  (check-equal? (secondary-index-keys (build-secondary-index col string<? 'none))
                #(("Bob" 4)
                  ("Faye" 9)
                  ("Jennie" 1)
                  ("Marnie" 10)
                  ("Webber" 14)))

  ; create a list of random numbers and index them
  (define xs (for/list ([_ (range 20)]) (random 10)))
  (define xix (build-secondary-index (build-column xs) <))
  
  ; ensure min, max, and mean
  (check-equal? (car (secondary-index-min xix)) (apply min xs))
  (check-equal? (car (secondary-index-max xix)) (apply max xs))
  (check-equal? (secondary-index-mean xix)
                (/ (apply + xs) (length xs)))

  ; fail empty index find
  (check-false (secondary-index-find (secondary-index empty-column #() <) 10)))
