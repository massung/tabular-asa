# Tabular Asa

A fast, efficient, in-memory, dataframes implementation for [Racket][racket].

# Example Usage

Here's a quick example of it in use:

```racket
(require (rename-in tabular-asa asa:))

; load a CSV file into a dataframe
(define books (asa:table-read/csv "test/books.csv"))

; index the books by publisher
(define ix (asa:table-index books 'Publisher string<?))

; get the list of indices for the publisher Penguin
(define indices (asa:secondary-index->stream ix #:from "Penguin" #:to "Penguin"))

; count the total "height" of all books published by Penguin
(let ([cut (asa:table-cut books '(Height))])
  (apply + (asa:table-map cut indices #:keep-index? #f)))

```

# fin.

[racket]: https://racket-lang.org/
