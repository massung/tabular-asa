#lang racket

(require racket/date)
(require racket/generic)

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define-generics orderable
  (ascending? orderable other)
  (descending? orderable other)
  #:fast-defaults
  ([boolean?
    (define (ascending? a b) (and a (not b)))
    (define (descending? a b) (or a (not b)))]
   [number?
    (define (ascending? a b) (or (not b) (< a b)))
    (define (descending? a b) (or (not b) (> a b)))]
   [string?
    (define (ascending? a b) (or (not b) (string<? a b)))
    (define (descending? a b) (or (not b) (string>? a b)))]
   [char?
    (define (ascending? a b) (or (not b) (char<? a b)))
    (define (descending? a b) (or (not b) (char>? a b)))]
   [sequence?
    (define (ascending? a b) (or (not b) (sequence<? a b)))
    (define (descending? a b) (or (not b) (sequence>? a b)))]
   [date?
    (define (ascending? a b) (or (not b) (< (date*->seconds a) (date*->seconds b))))
    (define (descending? a b) (or (not b) (> (date*->seconds a) (date*->seconds b))))]))

;; ----------------------------------------------------

(define (sequence<? xs ys)
  (cond
    [(null? ys) #f]
    [(null? xs) (not (null? ys))]
    [else       (let ([x (car xs)]
                      [y (car ys)])
                  (or (ascending? x y)
                      (and (equal? x y)
                           (sequence<? (cdr xs) (cdr ys)))))]))

;; ----------------------------------------------------

(define (sequence>? xs ys)
  (cond
    [(null? xs) #f]
    [(null? ys) (not (null? xs))]
    [else       (let ([x (car xs)]
                      [y (car ys)])
                  (or (descending? x y)
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
    (check-true (ascending? a b))
    (check-false (ascending? b a))

    (when b
      (check-true (descending? b a))
      (check-false (descending? a b))))

  ; test comparisons
  (test-case "ascending?"
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
