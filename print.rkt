#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require racket/pretty)

;; ----------------------------------------------------

(require "column.rkt")
(require "read.rkt")
(require "table.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide pretty-print-rows
         print-table
         display-table
         write-table)

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

(define (column-formatter k xs [mode #t])
  (let* ([repr (if mode ~v ~a)]

         ; maximum width of column name and values
         [width (for/fold ([w (string-length (repr k))])
                          ([x xs])
                  (max w (string-length (repr x))))])

    ; format function
    (λ (x) ((if mode ~v ~a) x
                            #:align 'right
                            #:width (+ width 3)
                            #:limit-marker "..."))))

;; ----------------------------------------------------

(define (write-table df
                     [port (current-output-port)]
                     [mode #t]
                     #:keep-index? [keep-index #t])
  (let* ([index-format (column-formatter '|| (index-preview df) mode)]

         ; formatters for each column
         [column-formats (for/list ([k (table-column-names df)])
                           (column-formatter k (column-preview df k) mode))]

         ; formatter for a row
         [row-format (λ (i row)
                       (when keep-index
                         (display (index-format i) port))
                       (for ([f column-formats]
                             [x row])
                         (display (f x) port))
                       (newline port))])

    ; write the header
    (row-format "" (table-column-names df))

    ; write all the column previews
    (for ([i (index-preview df)]
          [row (sequence-zip (for/list ([k (table-column-names df)])
                               (column-preview df k)))])
      (row-format i row))

    ; output the shape of the table
    (newline)
    (fprintf port "[~a rows x ~a cols]"
             (table-length df)
             (length (table-data df)))

    ; done
    (newline)))

;; ----------------------------------------------------

(define (display-table df [port (current-output-port)] #:keep-index? [keep-index #t])
  (write-table df port #f #:keep-index? keep-index))

;; ----------------------------------------------------

(define (print-table df [port (current-output-port)] #:keep-index? [keep-index #t])
  (write-table df port #t #:keep-index? keep-index))
