# Tabular Asa

A fast, efficient, in-memory, dataframes implementation for [Racket][racket].

# Example Usage

Here are a few quick examples of Tabular Asa in use.

```racket
(require plot)
(require tabular-asa)
(require threading)

; load a CSV file into a dataframe
(define books (call-with-input-file "books.csv" table-read/csv))

; pick a random title per genre
(group-sample (group-table/by books '(Genre)))

; find the longest books by publisher
(~> books 
    (table-drop-na '(Publisher))
    (table-sort '(Height) sort-descending)
    (table-distinct '(Publisher)))

; find all authors using more than one publisher
(let ([df (~> books
              (table-cut '(Author Publisher))
              (table-drop-na)
              (group-table/by '(Author))
              (group-unique))])
  (table-select df (table-apply (λ~> length (> 1)) df '(Publisher))))

; index the books by author and collect those rows
(let ([ix (build-index (table-column books 'Author))])
  (for*/list ([(genre indices) (index-scan-keys ix #:from "Huxley" #:to "M")]
              [i indices])
    (table-row books i)))

; plot how many books there are per genre
(let ([df (~> books
              (table-cut '(Genre Title))
              (group-table/by '(Genre))
              (group-count))])
  (plot (discrete-histogram (for/list ([x (table-column df 'Genre)]
                                       [y (table-column df 'Title)])
                              (list x y)))))
```

_NOTE: These examples make use of the [threading][threading] library for clarity of the code, but Tabular Asa doesn't require it as a dependency._

# fin.

[racket]: https://racket-lang.org/
[threading]: https://pkgs.racket-lang.org/package/threading
