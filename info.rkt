#lang info

(define collection "tabular-asa")
(define version "0.4.1")
(define pkg-authors '("massung@gmail.com"))
(define pkg-desc "A fast, efficient, immutable, dataframes implementation")
(define deps '("base" "rackunit-lib" "scribble-lib" "racket-doc" "scribble-doc" "csv-reading"))
(define scribblings '(("scribblings/tabular-asa.scrbl" ())))
(define compile-omit-paths '("examples"))
