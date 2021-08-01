#lang racket

(require racket/date)
(require racket/generic)

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define-generics comparable
  (less-than? comparable other)
  (greater-than? comparable other)
  #:fast-defaults
  ([boolean?
    (define (less-than? a b) (and a (not b)))
    (define (greater-than? a b) (or a (not b)))]
   [number?
    (define (less-than? a b) (or (not b) (< a b)))
    (define (greater-than? a b) (or (not b) (> a b)))]
   [string?
    (define (less-than? a b) (or (not b) (string<? a b)))
    (define (greater-than? a b) (or (not b) (string>? a b)))]
   [char?
    (define (less-than? a b) (or (not b) (char<? a b)))
    (define (greater-than? a b) (or (not b) (char>? a b)))]
   [sequence?
    (define (less-than? a b) (or (not b) (sequence<? a b)))
    (define (greater-than? a b) (or (not b) (sequence>? a b)))]
   [date?
    (define (less-than? a b) (or (not b) (< (date*->seconds a) (date*->seconds b))))
    (define (greater-than? a b) (or (not b) (> (date*->seconds a) (date*->seconds b))))]))

;; ----------------------------------------------------

(define (sequence<? xs ys)
  (cond
    [(null? ys) #f]
    [(null? xs) (not (null? ys))]
    [else       (let ([x (car xs)]
                      [y (car ys)])
                  (or (less-than? x y)
                      (and (equal? x y)
                           (sequence<? (cdr xs) (cdr ys)))))]))

;; ----------------------------------------------------

(define (sequence>? xs ys)
  (cond
    [(null? xs) #f]
    [(null? ys) (not (null? xs))]
    [else       (let ([x (car xs)]
                      [y (car ys)])
                  (or (greater-than? x y)
                      (and (equal? x y)
                           (sequence>? (cdr xs) (cdr ys)))))]))

;; ----------------------------------------------------

(module+ test
  (require rackunit)

  ; create a couple dates
  (define d1 (current-date))
  (define d2 (begin
               (sleep 0.01)
               (current-date)))

  ; test positive and negative
  (define (check<? a b)
    (check-true (less-than? a b))
    (check-false (less-than? b a))

    (when b
      (check-true (greater-than? b a))
      (check-false (greater-than? a b))))

  ; test comparisons
  (test-case "less-than?"
             (check<? 1.0 2)
             (check<? "abc" "def")
             (check<? "ABC" "abc")
             (check<? #\a #\b)
             (check<? #\A #\B)
             (check<? '(1 2 3) '(1 2 4))
             (check<? '() '(1 2 3))
             (check<? d1 d2)
             (check<? 10 #f)
             (check<? "a" #f)
             (check<? #\a #f)
             (check<? '(1) #f)
             (check<? d1 #f)
             (check<? #t #f)))
