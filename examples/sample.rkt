#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require plot)
(require tabular-asa)
(require threading)

; load a CSV file into a dataframe
(define books (call-with-input-file "books.csv" table-read/csv))

; plot how many books there are per genre
(let ([df (~> books
              (table-drop-na '(Publisher))  ; remove rows with missing publisher
              (table-cut '(Genre Title))    ; keep only genre and title columns
              (table-groupby '(Genre))      ; group records by genre
              (group-count))])              ; aggregate titles by genre
  (parameterize ([plot-x-tick-label-angle 30]
                 [plot-x-tick-label-anchor 'top-right])
    (plot (discrete-histogram (for/list ([x (table-column df 'Genre)]
                                         [y (table-column df 'Title)])
                                (list x y)))
          #:x-label "Genre"
          #:y-label "Number of Titles Published")))
