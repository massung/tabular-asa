# Tabular Asa

A fast, efficient, in-memory, dataframes implementation for [Racket][racket].

# Example Usage

Here are a few quick examples of Tabular Asa in use.

```racket
(require tabular-asa)
(require threading)

; load a CSV file into a dataframe
(define books (call-with-input-file "test/books.csv" table-read/csv))

; pick a random title per genre
(group-sample (group-table/by books 'Genre))

; find the longest books by publisher
(~> books 
    (table-drop-na '(Publisher))
    (table-sort '(Height) sort-descending)
    (table-distinct '(Publisher)))

; count how many books of each genre by publisher
(~> books
    (table-cut '(Publisher Genre Title))
    (group-table/by '(Publisher Genre))
    (group-count))

; index the books by author and collect those rows
(let ([ix (build-index (table-column books 'Author))])
  (for*/list ([(author indices) (index-scan-keys ix #:from "J" #:to "R")]
              [i indices])
    (table-irow books i)))
```

_NOTE: These examples make use of the [threading][threading] library for clarity of the code, but Tabular Asa doesn't require it as a dependency._

# fin.

[racket]: https://racket-lang.org/
[threading]: https://pkgs.racket-lang.org/package/threading
