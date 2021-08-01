#lang racket

(require "column.rkt")
(require "compare.rkt")
(require "for.rkt")
(require "index.rkt")
(require "read.rkt")
(require "table.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(struct group [table by index]
  #:property prop:sequence
  (λ (g) (group-table g))

  ; custom printing
  #:methods gen:custom-write
  [(define write-proc
     (λ (g port mode)
       (fprintf port "#<group ~a [~a rows x ~a cols]>"
                (group-by g)
                (index-length (group-index g))
                (length (table-data (group-table g))))))])

;; ----------------------------------------------------

(define (table-group df by)
  (let ([col (table-column df by)])
    (group (table-drop df (list by)) by (build-index col #f))))

;; ----------------------------------------------------

(define (group-fold proc init g #:result [result identity])
  (let* ([xs-columns (table-column-names (group-table g))]
         [xs-init (map (const init) xs-columns)])
    (for/table ([columns (cons (group-by g) xs-columns)])
               ([(key indices) (group-index g)])
      (cons key (for/fold ([xs xs-init] #:result (map result xs))
                          ([i indices])
                  (map proc xs (cdr (table-row (group-table g) i))))))))

;; ----------------------------------------------------

(define (group-count g)
  (group-fold (λ (a b) (if b (add1 a) a)) 0 g))

;; ----------------------------------------------------

(define (group-min g [less-than? less-than?])
  (group-fold (λ (a b) (if (or (not a) (less-than? a b)) a b)) #f g))

;; ----------------------------------------------------

(define (group-max g [less-than? less-than?])
  (group-fold (λ (a b) (if (or (not a) (less-than? a b)) b a)) #f g))

;; ----------------------------------------------------

(define (group-mean g)
  (let ([agg (λ (a b)
               (cond
                 [(not b) a]
                 [(not a) (list b 1)]
                 [else    (list (+ (first a) b)
                                (+ (second a) 1))]))])
    (group-fold agg #f g #:result (λ (pair) (apply / pair)))))

;; ----------------------------------------------------

(module+ test
  (require rackunit)
  (require "print.rkt")

  ; create a simple table
  (define birds (table-read/sequence '(("Crane" 4.3 7.3)
                                       ("Crane" 5.2 7.5)
                                       ("Egret" 2.6 4.3)
                                       ("Egret" 3.4 5.5)
                                       ("Heron" 3.2 5.5)
                                       ("Heron" 5.5 6.6)
                                       ("Stork" 3.3 5.0)
                                       ("Stork" 3.8 5.0))
                                     '(bird length wingspan)))

  ; group by bird
  (define g (table-group birds 'bird))

  ; min length by bird
  (let ([df (group-mean (table-group birds 'bird))])
    (display-table df))
  )
