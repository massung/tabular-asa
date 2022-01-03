#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require csv-reading
         json)

;; ----------------------------------------------------

(require "table.rkt")

;; ----------------------------------------------------

(provide (all-defined-out))

;; ----------------------------------------------------

(define table-builder%
  (class object%
    (super-new)

    ; initialization
    (init-field [initial-size 5000]
                [columns '()]
                [sort-columns #f])

    ; initial columns from header
    (define column-data (make-hasheq))
    (define column-order (for/list ([k columns])
                           (string->symbol (format "~a" k))))

    ; table size and next row index
    (define n initial-size)
    (define i 0)

    ; an infinite sequence of generated column names
    (define new-columns (sequence-map (λ (_) (gensym "col")) (in-naturals)))

    ; create a new column
    (define/public (add-column name [backfill #f])
      (let ([column-name (if (symbol? name)
                             name
                             (string->symbol (~a name)))])
        (set! column-order (append column-order (list column-name))))
      (make-vector n backfill))

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

    ; append a value to a column
    (define/private (column-set! k x)
      (let ([v (hash-ref! column-data k (λ () (add-column k)))])
        (vector-set! v i x)))

    ; append a record
    (define/public (add-record record)
      (for ([(k x) record])
        (column-set! k x))
      (grow-table))
    
    ; append a row
    (define/public (add-row xs [ks #f])
      (for ([k (sequence-append (or ks column-order) new-columns)] [x xs])
        (column-set! k x))
      (grow-table))

    ; build the final table
    (define/public (build)
      (when sort-columns
        (set! column-order (sort column-order symbol<?)))
      (table (build-vector i identity)
             (for/list ([k column-order])
               (let ([v (hash-ref column-data k)])
                 (cons k (vector-take v i))))))))

;; ----------------------------------------------------

(define (table-read/sequence seq [columns '()])
  (let ([builder (new table-builder% [columns columns])])
    (for ([row seq])
      (send builder add-row row))
    (send builder build)))

;; ----------------------------------------------------

(define (table-read/columns seq [columns #f])
  (for/fold ([df empty-table])
            ([xs seq]
             [col (or columns (in-cycle (list #f)))])
    (table-with-column df xs #:as col)))

;; ----------------------------------------------------

(define (table-read/jsexpr jsexpr)
  (let ([builder (new table-builder%
                      [sort-columns #t])])
    (match jsexpr
            
      ; column-oriented object -- {"col": [x1, x2, ...], ...}
      [(hash-table (ks (list xs ...)) ...)
       (letrec ([keys (hash-keys jsexpr)]

                ; recursively add the rows together
                [add-rows (λ (cols)
                            (unless (empty? (first cols))
                              (send builder add-row (map first cols) keys)
                              (add-rows (map rest cols))))])
         (add-rows (hash-values jsexpr)))]

      ; row-oriented list -- [{"col1": x, "col2": y, ...}, ...]
      [(list records ...)
       (for ([r records])
         (send builder add-record r))]
                
      ; unknown json orientation
      [else #f])

    ; construct the table - order the columns by name
    (let ([df (send builder build)])
      (table-cut df (sort (table-header df) symbol<?)))))

;; ----------------------------------------------------

(define (table-read/csv port
                        #:header? [header #t]
                        #:drop-index? [drop-index #f]
                        #:separator-char [sep #\,]
                        #:newline [newline 'lax]
                        #:quote-char [quote #\"]
                        #:double-quote? [double-quote #t]
                        #:comment-char [comment #\#]
                        #:strip? [strip #f]
                        #:na [na #f]
                        #:na-values [na-values (list "" "-" "." "na" "n/a" "nan" "null")])
  (let* ([spec `((separator-chars ,sep)
                 (newline-type . ,newline)
                 (quote-char . ,quote)
                 (quote-doubling-escapes? . ,double-quote)
                 (comment-chars ,comment)
                 (strip-leading-whitespace? . ,strip)
                 (strip-trailing-whitespace? . ,strip))]
             
         ; create parser
         [next-row (let ([reader (make-csv-reader port spec)])
                     (if (not drop-index)
                         reader
                         (λ ()
                           (let ([row (reader)])
                             (if (empty? row) row (cdr row))))))]

         ; get the first row (optionally the header)
         [first-row (next-row)]

         ; create a list of column names/indices
         [columns (if header
                      first-row
                      (for/list ([_ first-row])
                        (gensym "col")))]

         ; parse cells, look for n/a as well
         [parse-row (λ (r)
                      (for/list ([x r])
                        (let ([n (string->number x)])
                          (cond
                            [n n]
                            [(member x na-values string-ci=?) na]
                            [else x]))))])

    ; read each row into a new table
    (table-read/sequence (csv-map parse-row next-row) columns)))

;; ----------------------------------------------------

(define (table-read/json port #:lines? [lines #f])
  (if lines
      (let ([builder (new table-builder% [sort-columns #t])])
        (do ([r (read-json port)
                (read-json port)])
          [(eof-object? r) (send builder build)]
          (send builder add-record r)))

      ; read the entire json first
      (table-read/jsexpr (read-json port))))
