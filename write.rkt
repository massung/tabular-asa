#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require file/gzip
         json)

;; ----------------------------------------------------

(require "column.rkt")
(require "print.rkt")
(require "read.rkt")
(require "table.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define (table-write/string df
                            [port (current-output-port)]
                            #:keep-index? [keep-index #t])
  (parameterize ([pretty-print-rows #f])
    (display-table df port #:keep-index? keep-index)))

;; ----------------------------------------------------

(define (table-write/csv df
                         [port (current-output-port)]
                         #:keep-index? [keep-index #t]
                         #:header? [header #t]
                         #:separator-char [sep #\,]
                         #:quote-char [quote #\"]
                         #:escape-char [escape #\\]
                         #:list-char [list-sep #\|]
                         #:double-quote? [double-quote #t]
                         #:na-rep [na ""]
                         #:na-values [na-values (list #f)])
  (letrec ([esc-quote (if double-quote
                          (string quote quote)
                          (string escape quote))]

           ; output a single cell
           [write-cell (λ (s)
                         (if (string-contains? s (string sep))
                             (let ([qs (string-replace s (string quote) esc-quote)])
                               (fprintf port "~a~a~a" quote qs quote))
                             (display s port)))]

           ; output a separated sequence of values
           [write-row (λ (x . xs)
                        (write-cell (if (member x na-values) na (~a x)))
                        (if (empty? xs)
                            (newline port)
                            (begin
                              (display sep port)
                              (apply write-row xs))))])

    ; write the header?
    (when header
      (let ([cols (table-column-names df)])
        (apply write-row (if keep-index (cons "" cols) cols))))

    ; write each row
    (for ([row df])
      (apply write-row (if keep-index row (cdr row))))))

;; ----------------------------------------------------

(define (table-write/json df
                          [port (current-output-port)]
                          #:orient [orient 'records]
                          #:lines? [lines #f]
                          #:na-rep [na (json-null)])
  (parameterize ([json-null na])
    (case orient
      ('records
       (unless lines
         (display #\[ port))

       ; write all the records, in lines mode use newlines instead of commas
       (let ([s (sequence->stream (table-records df))])
         (unless (stream-empty? s)
           (write-json (stream-first s) port)

           ; separate the rest of the records
           (do ([s (stream-rest s)
                   (stream-rest s)])
             [(stream-empty? s)]
             (if lines
                 (newline port)
                 (display #\, port))
             (write-json (stream-first s) port))))

       ; terminate the array
       (unless lines
         (display #\] port)))

      ; {"col": [x1, x2, ...], ...}
      ('columns
       (let ([jsexpr (for/hash ([col (table-columns df)])
                       (values (column-name col)
                               (sequence->list (column-data col))))])
         (write-json jsexpr port)))
  
      ; unknown format
      (else (error "Unknown record format:" orient)))))
