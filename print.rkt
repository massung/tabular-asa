#lang racket

(require "index.rkt")
(require "read.rkt")
(require "table.rkt")

;; ----------------------------------------------------

(provide table-print-size
         print-table
         display-table
         write-table)

;; ----------------------------------------------------

(define table-print-size (make-parameter 10))

;; ----------------------------------------------------

(define (column-preview df k [n (table-print-size)])
  (if (or (not n) (<= (table-length df) n))
      (index->stream (table-column df k))
      (let ([n (quotient n 2)])
        (stream-append (index->stream (table-column (table-head df n) k))
                       (stream "...")
                       (index->stream (table-column (table-tail df n) k))))))

;; ----------------------------------------------------

(define (index-preview df [n (table-print-size)])
  (if (or (not n) (<= (table-length df) n))
      (index->stream (table-index df))
      (let ([n (quotient n 2)])
        (stream-append (index->stream (table-index (table-head df n)))
                       (stream "..")
                       (index->stream (table-index (table-tail df n)))))))

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
  (let* ([index-format (column-formatter (index-column) (index-preview df) mode)]

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
    (row-format (index-column) (table-column-names df))

    ; write all the column previews
    (letrec ([zip-columns (λ (xs)
                            (match xs
                              [(list x)
                               (for/stream ([e x]) (list e))]
                              [(list x xs ...)
                               (for/stream ([e x] [e* (zip-columns xs)])
                                 (cons e e*))]))])
      (for ([i (index-preview df)]
            [row (zip-columns (for/list ([k (table-column-names df)])
                                (column-preview df k)))])
        (row-format i row)))))

;; ----------------------------------------------------

(define (display-table df [port (current-output-port)] #:keep-index? [keep-index #t])
  (write-table df port #f #:keep-index? keep-index))

;; ----------------------------------------------------

(define (print-table df [port (current-output-port)] #:keep-index? [keep-index #t])
  (write-table df port #t #:keep-index? keep-index))