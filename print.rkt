#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require "table.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide pretty-print-rows
         format-table
         print-table
         display-table)

;; ----------------------------------------------------

(define pretty-print-rows (make-parameter 10))

;; ----------------------------------------------------

(define (column-preview df k [n (pretty-print-rows)])
  (if (or (not n) (<= (table-length df) n))
      (table-column df k)
      (let ([n (quotient n 2)])
        (sequence-append (table-column (table-head df n) k)
                         (list "...")
                         (table-column (table-tail df n) k)))))

;; ----------------------------------------------------

(define (index-preview df [n (pretty-print-rows)])
  (if (or (not n) (<= (table-length df) n))
      (table-index df)
      (let ([n (quotient n 2)])
        (sequence-append (table-index (table-head df n))
                         (list "..")
                         (table-index (table-tail df n))))))

;; ----------------------------------------------------

(define (column-gap-preview [n (pretty-print-rows)])
  (sequence-map (const "...") (in-range n)))

;; ----------------------------------------------------

(define (column-formatter k xs [repr ~a])
  (let* ([width (for/fold ([w (string-length (repr k))])
                          ([x xs])
                  (max w (string-length (repr x))))])

    ; format function
    (λ (x) (repr x
                 #:align 'right
                 #:width (+ width 3)
                 #:limit-marker "..."))))

;; ----------------------------------------------------

(define (format-table df port mode #:keep-index? [keep-index #t])
  (let* ([repr (if mode ~v ~a)]

         ; how to display the index column
         [index-format (column-formatter '|| (index-preview df) repr)]

         ; formatters for each column
         [column-formats (for/list ([k (table-header df)])
                           (column-formatter k (column-preview df k) repr))]

         ; formatter for a row
         [row-format (λ (i row)
                       (when keep-index
                         (display (index-format i) port))
                       (for ([f column-formats]
                             [x row])
                         (display (f x) port))
                       (newline port))])

    ; write the header
    (row-format '|| (table-header df))

    ; write all the column previews
    (for ([i (index-preview df)]
          [row (sequence-zip (for/list ([k (table-header df)])
                               (column-preview df k)))])
      (row-format i row))

    ; output the shape of the table
    (newline port)
    (table-preview-shape df port #f)))

;; ----------------------------------------------------

(define (display-table df [port (current-output-port)] #:keep-index? [keep-index #t])
  (format-table df port #f #:keep-index? keep-index))

;; ----------------------------------------------------

(define (print-table df [port (current-output-port)] #:keep-index? [keep-index #t])
  (format-table df port #t #:keep-index? keep-index))
