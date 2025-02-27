#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require plot)
(require tabular-asa)
(require racket/runtime-path)

; set the current path
(define-runtime-path here ".")

; load a CSV file into a dataframe
(define books (call-with-input-file (build-path here "books.csv") table-read/csv))

; keep only the genre and title columns
(define genre-title (table-cut books '(Genre Title)))

; group the titles by genre
(define grouped (table-groupby genre-title '(Genre)))

; count published titles by group
(define titles (group-count grouped))

; plot the published titles
(parameterize ([plot-x-tick-label-angle 30]
               [plot-x-tick-label-anchor 'top-right])
  (plot (discrete-histogram (for/list ([x (table-column titles 'Genre)]
                                       [y (table-column titles 'Title)])
                              (list x y)))
        #:x-label "Genre"
        #:y-label "Number of Titles Published"))
