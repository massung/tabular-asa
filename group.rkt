#lang racket

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
         [start (if from (secondary-index-find ix from #f) 0)]
         [end (if to (secondary-index-find ix to #f) n)])
    (if (<= (- end start) 0)
        (secondary-index-stream 0 0 #() #f)
        (secondary-index-stream start
                                end
                                (secondary-index-keys ix)
                                (cdr (secondary-index-ref ix start))))))
  
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
                               [(>= (add1 i) end)
                                (if exact #f end)]

                               ; search the left or right half?
                               [else (if (less-than? key (car group))
                                         (let ([ni (arithmetic-shift (+ start i) -1)])
                                           (search ni start i))
                                         (let ([ni (arithmetic-shift (+ i end) -1)])
                                           (search ni i end)))])))])
          (search (arithmetic-shift n -1) 0 n)))))

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
  (/ (for/sum ([x (secondary-index-keys ix)]) x) (secondary-index-length ix)))

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
  