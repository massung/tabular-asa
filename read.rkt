#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require csv-reading
         file/gunzip
         json)

;; ----------------------------------------------------

(require "column.rkt")
(require "table.rkt")
(require "utils.rkt")

;; ----------------------------------------------------

(provide (all-defined-out)
         (except-out table-read))

;; ----------------------------------------------------

(define table-builder%
  (class object%
    (super-new)

    ; initialization
    (init-field [initial-size 5000]
                [columns '()])

    ; initial columns from header
    (define column-data (make-hasheq))
    (define column-order (for/list ([k columns])
                           (string->symbol (format "~a" k))))

    ; table size and next row index
    (define n initial-size)
    (define i 0)

    ; create a new column
    (define/public (add-column name)
      (let ([column-name (string->symbol (~a name))])
        (set! column-order (append column-order (list column-name))))
      (make-vector n #f))

    ; build the column-major data vectors
    (for ([k column-order])
      (hash-set! column-data k (make-vector n #f)))

    ; advance the row index, increase table size if needed
    (define/private (grow-table)
      (set! i (add1 i))

      ; grow all columns if needed
      (unless (< i n)
        (for ([k (hash-keys column-data)])
          (hash-update! column-data k (λ (v)
                                        (vector-append v (make-vector n #f)))))
        
        ; increase the size of the table
        (set! n (+ n n))))

    ; append a record
    (define/public (add-record record)
      (for ([(k x) record])
        (let ([v (hash-ref! column-data k (λ () (add-column k)))])
          (vector-set! v i x)))
      (grow-table))
    
    ; append a row
    (define/public (add-row xs [ks #f])
      (for ([k (or ks column-order)] [x xs])
        (let ([v (hash-ref! column-data k (λ () (add-column k)))])
          (vector-set! v i x)))
      (grow-table))

    ; build the final table
    (define/public (build)
      (table (build-vector i identity)
             (for/list ([k column-order])
               (cons k (vector-take (hash-ref column-data k) i)))))))

;; ----------------------------------------------------

(define (table-read port-or-filename read-proc)
  (cond
    [(input-port? port-or-filename)
     (read-proc port-or-filename)]

    ; TODO: (eq? port-or-filename 'stdin)

    ; local file, open for reading and parse
    [(string? port-or-filename)
     (call-with-input-file port-or-filename read-proc)]

    ; unknown import source
    [#t (error "Invalid input-port or filename:" port-or-filename)]))

;; ----------------------------------------------------

(define (table-read/csv port-or-filename
                        #:header? [header #t]
                        #:separator-char [sep #\,]
                        #:newline [newline 'lax]
                        #:quote-char [quote #\"]
                        #:double-quote? [double-quote #t]
                        #:comment-char [comment #\#]
                        #:strip? [strip #f]
                        #:na [na #f]
                        #:na-values [na-values (list "" "." "na" "n/a" "nan" "null")])
  (table-read port-or-filename
              (λ (port)
                (let* ([spec `((separator-chars ,sep)
                               (newline-type . ,newline)
                               (quote-char . ,quote)
                               (quote-doubling-escapes? . ,double-quote)
                               (comment-chars ,comment)
                               (strip-leading-whitespace? . ,strip)
                               (strip-trailing-whitespace? . ,strip))]
             
                       ; create parser
                       [next-row (make-csv-reader port spec)]

                       ; get the first row (optionally the header)
                       [first-row (next-row)]

                       ; create a list of column names/indices
                       [column-names (if header first-row (range (length first-row)))]

                       ; parse cells, look for n/a as well
                       [parse-row (λ (r)
                                    (for/list ([x r])
                                      (let ([n (string->number x)])
                                        (cond
                                          [n n]
                                          [(member x na-values string-ci=?) na]
                                          [else x]))))]

                       ; initialize a new table builder
                       [builder (new table-builder% [columns column-names])])

                  ; add all the rows to the table
                  (csv-for-each (λ (r) (send builder add-row (parse-row r))) next-row)

                  ; return the table
                  (send builder build)))))

;; ----------------------------------------------------

(define (table-read/json port-or-filename #:lines? [lines #f])
  (table-read port-or-filename
              (λ (port)
                (let ([builder (new table-builder%)])
                  (if lines

                      ; read each line as a single record
                      (do ([r (read-json port)
                              (read-json port)])
                        [(eof-object? r)]
                        (send builder add-record r))

                      ; read the entire json first
                      (let ([x (read-json port)])
                        (match x
                
                          ; column-oriented object -- {"col": [x1, x2, ...], ...}
                          [(hash-table (ks (list xs ...)) ...)
                           (letrec ([keys (hash-keys x)]

                                    ; recursively add the rows together
                                    [add-rows (λ (cols)
                                                (unless (empty? (first cols))
                                                  (send builder add-row (map first cols) keys)
                                                  (add-rows (map rest cols))))])
                             (add-rows (hash-values x)))]

                          ; row-oriented list -- [{"col1": x, "col2": y, ...}, ...]
                          [(list records ...)
                           (for ([r records])
                             (send builder add-record r))]
                
                          ; unknown json orientation
                          [else #f])))

                  ; construct the table
                  (send builder build)))))
