#lang racket

(require (rename-in tabular-asa asa:))

;; ----------------------------------------------------

(define books (call-with-input-port "books.csv" table-read/csv))

;; ----------------------------------------------------

