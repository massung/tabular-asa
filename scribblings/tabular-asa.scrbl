#lang scribble/manual

@require[@for-label[tabular-asa racket/base racket/class]]

@title{Tabular Asa}
@author[@author+email["Jeffrey Massung" "massung@gmail.com"]]

@defmodule[tabular-asa]

A fast, efficient, immutable, dataframes implementation.


@;; ----------------------------------------------------
@section{Sources}

The source code can be found at @url{https://github.com/massung/tabular-asa}.


@;; ----------------------------------------------------
@section{Quick Example}

This is a brief example of loading a table from a CSV, filtering, grouping, aggregating, and plotting the data. @italic{Note: This example uses @racket[~>] from the @hyperlink["https://docs.racket-lang.org/threading/index.html"]{threading} module for clarity, but Tabular Asa does not require it.}

@racketblock[
 (define books (call-with-input-file "books.csv" table-read/csv))

 (let ([df (~> books
               (table-drop-na '(Publisher))
               (table-cut '(Genre Title))
               (table-groupby '(Genre))
               (group-count))])
   (parameterize ([plot-x-tick-label-angle 30]
                  [plot-x-tick-label-anchor 'top-right])
     (plot (discrete-histogram (for/list ([x (table-column df 'Genre)]
                                          [y (table-column df 'Title)])
                                 (list x y)))
           #:x-label "Genre"
           #:y-label "Number of Titles Published")))
]

@image{examples/plot.png}


@;; ----------------------------------------------------
@section{Introduction}

Tabular Asa is intended to fulfill the following goals:

@itemlist[
 @item{Be as efficient as possible sharing memory.}
 @item{Be as lazy as possible; use sequences and streams.}
 @item{Be usable for everyday tabular data tasks.}
 @item{Be extremely simple to understand and extend.}
 @item{Be flexible by exposing low-level functionality.}
 @item{Be a learning example for students who want to learn how to implement databases.}
]

Tabular Asa does this by starting with a couple very simple concepts and building on them. In order, those concepts are:

@itemlist[
 @item{Columns.}
 @item{Tables.}
 @item{Indexing.}
 @item{Grouping.}
 @item{Aggregating.}
]

@;; ----------------------------------------------------
@section{Row vs. Column Major}

When thinking about tabular data, it's very common to think of each row (or record) as a thing to be grouped together. However, this is extremely inefficient for most operations; it requires extracting data from a larger collection into a smaller collection for many operations. It is also an inefficient use of cache. For this reason Tabular Asa is column-major.

A simple example of this difference in implementation would be cutting or inserting columns (a SELECT operation in SQL) to a table. Consider the following table of data:

@tabular[#:style 'boxed
         #:column-properties '(left left left)
         #:row-properties '(bottom-border () () ())
         (list (list @bold{name} @bold{age} @bold{gender})
               (list "Jeff" "23" "m")
               (list "Sam" "14" "m")
               (list "Kate" "38" "f"))]

When rows are stored as a sequence or hash, removing or adding a column requires duplicating every single row of data and copying it into a new sequence or hash, essentially doubling the memory usage and increasing the time it takes to perform the operation. However, if the table is stored as 3 columns, then creating a new table with a column added only adds the memory cost of the new column. Selecting a subset of columns is even easier.

Additionally, every table contains a vector which is the index of which rows it contains from the original column data. This allows for tables that are filters to simply reference the existing column data, but with a new index. In the above example table, the index would be the vector @racket[#(0 1 2)]. If a new table was generated by filtering the original, keeping only the girls, then the new table would contain all the same column data (referencing the exact same columns in memory), but the index would be @racket[#(2)].


@;; ----------------------------------------------------
@section{Row vs. Record}

For the purposes of this documentation and function names, a "row" is defined as a @racket[list?] and a "record" is defined as @racket[hash?].

Likewise, the symbol @racket[k] is used in place of a column name (a @racket[symbol?]) and the symbol @racket[ks] is used for a list of column names.


@;; ----------------------------------------------------
@section{Reading Tables}

It is important to note that - when reading tables - columns that don't already exist will be generated on-demand. The column names will be equivelant to calling @racket[(gensym "col")] to ensure they are unique symbols.

@defproc[(table-read/sequence [seq (or/c (listof any/c)
                                         (sequenceof hash-eq?))]
                              [columns (listof symbol?) '()])
         table?]{
 A versatile function that builds a @racket[table] from a sequence of rows or records. Each item in the sequence can be one of the following:

 @itemlist[
  @item{A list of values}
  @item{An associative list of @racket[(col value)] pairs}
  @item{A @racket[hash-eq?] mapping columns to values}
 ]

 If the @racket[columns] parameter is supplied, that will be used as the initial set of column names supplied to the @racket[table-builder%]. This can be especially useful when supplying hashes to guarantee the column order.
}

@defproc[(table-read/columns [seq (sequenceof sequence?)]
                             [columns (or/c (listof symbol?) #f) #f])
         table?]{
 Creates and returns a new @racket[table] from a series of sequences. If @racket[columns] is provided then each column will be given the name, otherwise generated column names will be provided.

 @racketblock[
  (table-read/columns '((0 1 2) #("a" "b" "c") "def") '(col1 col2 col3))
 ]
}

@defproc[(table-read/jsexpr [jsexpr jsexpr?]) table?]{
 Given a @racket[jsexpr?], use the shape of the object to determine how it should be transformed into a table.

 If @racket[jsexpr] is a JSON object (@racket[hash-eq?]), then it is assumed to be a hash of columns, where each column contains the values for it.

 If @racket[jsexpr] is a JSON array (@racket[list?]), then it is assumes to be a list of @racket[hash-eq?] records, where the key/value pairs are the column names and values for each row of the table.
}

@defproc[(table-read/csv [port input-port?]
                         [#:header? header boolean? #t]
                         [#:drop-index? drop-index boolean? #f]
                         [#:separator-char sep char? #\,]
                         [#:quote-char quote char? #\"]
                         [#:double-quote? double-quote char? #t]
                         [#:comment-char comment char? #\#]
                         [#:strip? strip boolean? #f]
                         [#:readers readers (listof (string? -> any/c)) (list string->number)]
                         [#:na na any/c #f]
                         [#:na-values na-values (listof string?) (list "" "-" "." "na" "n/a" "nan" "null")])
         table?]{
 Reads the data in @racket[port] as a CSV file using the options specified and returns a new @racket[table]. Most of the arguments are used for parsing the CSV.

 The @racket[header] argument - if @racket[#t] - indicates that the first non-comment row of the CSV should be treated as the list of column names. If @racket[#f] then the column names will be generated as needed.

 The @racket[drop-index] argument - if @racket[#t] - assumes that the first column of the CSV is the row index (i.e., an auto-incrementing integer) and shouldn't be kept. If there is a row index column, and it is not dropped, it's important to note that it will be treated just like any other column and is NOT used as the table's index.
 
 The @racket[na-values] argument is a list of strings that - when parsed as the value for a given cell - are replaced with the @racket[na] value to indicate N/A (not available). The values in this list are case-insensitive.
 
 The @racket[readers] argument - since CSVs are untyped, this is the list of type transforms that will be attempted on every cell, in order. The first one to successfully return a value will be the value in the cell. If none succeed, then the type of the cell is just a string. You can override this list to include functions from other packages like @racket[iso8601->date]. Similarly, you can supply an empty list to ensure all cells are either strings or the @racket[na] value. It should be noted that this list of transform readers is applied to all columns. In the future this may change to allow a @racket[hash], where each column can have its own list of transform readers.
}

@defproc[(table-read/json [port input-port?]
                          [#:lines? lines boolean? #f])
         table?]{
 Reads the data in @racket[port] as a JSON value. If @racket[lines] is @racket[#t] then the @racket[port] is read line-by-line, where each line is assumed to be a JSON object (@racket[hash-eq?]) corresponding to a single record of the resulting table. Otherwise the entire JSON object is read into memory and passed to @racket[table-read/jsexpr].
}


@;; ----------------------------------------------------
@section{Building Tables}

Tables can also be built at constructed using an instance of @racket[table-builder%] or with the @racket[for/table] macro.

@defclass[table-builder% object% ()]{
 A @racket[table-builder%] is an object that can be sent rows or records for appending and automatically grow the shape of the table being built efficiently.

 @defconstructor[([initial-size exact-nonnegative-integer? 5000]
                  [columns (listof symbol?) '()]
                  [sort-columns boolean? #f])]{
  Creates a new @racket[table-builder%] with an initial shape.

  The @racket[initial-size] is how many rows are initially reserved for each column.

  The @racket[columns] is the initial list (and order) of column names. Columns may be added dynamically as rows and records are appended to the table.

  The @racket[sort-columns] parameter makes it so that - upon building the table - the columns are sorted alphabetically. This is useful when building a table from records and you want to ensure a consistent ordering.
 }

 @defmethod[(add-column [name symbol?]
                        [backfill any/c #f])
            void?]{
  Appends a new column to the table being built. Any rows already added to the table will be be backfilled with @racket[backfill].

  Typically, this method need not be called manually, as it will automatically be called as-needed by @racket[add-row] and @racket[add-record].
 }

 @defmethod[(add-row [r list?]
                     [ks (or/c (non-empty-listof symbol?) #f)])
            void?]{
  Appends a new row of values to the table. If @racket[ks] is @racket[#f] (the default), then the current set of columns (in order) of the table is assumed. If the row contains more values than there are columns then additional columns will be added to the table with generated names.
 }

 @defmethod[(add-record [r hash-eq?]) void?]{
  Appends a new row of values to the table. The record is assumed to be a @racket[hash-eq?] of @racket[(k . value)] pairings. If the record contains a column name not yet present in the table, a new column is created for it.
 }

 @defmethod[(build) table?]{
  Builds a new index for the table, truncates the column data and returns it.

  This method may be called more than once, and each time it will return a new table. This allows you to do things like add rows, build a table, add more rows and columns, build another table, etc.
 }

 Example:

 @racketblock[
  (let ([builder (new table-builder%
                      [columns '(hero universe)])])
    (send builder add-row '("Superman" "DC"))
    (send builder add-record #hasheq((hero . "Wolverine") (universe . "Marvel")))
    (send builder build))
 ]
}

@defform[(for/table (init-forms ...) (for-clause ...) body-or-break ... body)]{
 Builds a table using a @racket[for] macro.
 
 The @racket[init-forms] are the optional, initial forms used to create a @racket[table-builder%] instance.

 Each iteration of @racket[body] should return either a @racket[list?] or @racket[hash-eq?], which will automatically be sent to the @racket[table-builder%] using @racket[add-row] or @racket[add-record]. It's also possible to mix and match (i.e., return a list for one iteration and a hash for another).

 When the @racket[for-clause] terminates, the table is built and returned.

 Example:

 @racketblock[
  (for/table ([initial-size 3]
              [columns '(hero universe)])
             ([i 3])
    (case i
     ((0) '("Superman" "DC"))
     ((1) '("Wolverine" "Marvel"))
     ((2) '("Batman" "DC"))))
 ]
}


@;; ----------------------------------------------------
@section{Tables}

@defstruct[table ([index (vectorof exact-nonnegative-integer?)]
                  [data (listof (cons/c symbol? (vectorof any/c)))])]{
 The constructor for a new table structure. There should almost never be a need to call this directly as opposed to using one of the table-read/* functions to load a table from another container or a port.

 All tables are also sequences and can be iterated using @racket[for], where each iteration returns the next index and row (list). For example:

 @racketblock[
  (define df (table #(0 1 2)
                    '((hero . #("Superman" "Batman" "Wonder Woman"))
                      (gender . #(m m f)))))
  (for ([(i row) df])
    (displayln row))
 ]
}

@defthing[empty-table table?]{
 An immutable, empty table. Useful for building a table from scratch using @racket[table-with-column] or returning from a function in failure cases, etc.
}

@defproc[(table-preview-shape [df table?]
                              [port output-port?]
                              [mode boolean?])
         void?]{
 Prints the shape of the table to the given port on a single line like so:

 @verbatim|{[359 rows x 8 cols]}|.

 The @racket[mode] argument is currently ignored.
}

@defparam[table-preview proc (table? output-port? -> void?) #:value procedure?]{
  Controls how tables are previewed on the REPL. The default function is @racket[table-preview-shape].

  You may supply your own function, or even replace it with @racket[format-table] if you always want to see a preview of the table on the REPL.
}

@defproc[(table-length [df table?]) exact-nonnegative-integer?]{
 Returns the number of rows in the table.
}

@defproc[(table-shape [df table?])
         (values exact-nonnegative-integer?
                 exact-nonnegative-integer?)]{
 Returns the number of rows and columns in the table as multiple values.
}

@defproc[(table-empty? [df table?]) boolean?]{
 Returns @racket[#t] if there are no rows or no columns in the table.
}

@defproc[(table-header [df table?]) (listof symbol?)]{
 Returns a list of symbols, which are the column names of the table.
}

@defproc[(table-columns [df table?]) (listof column?)]{
 Returns a list of columns containing all the data in the table.
}

@defproc[(table-column [df table?]
                       [k symbol?])
         column?]{
 Looks up the column named @racket[k] and returns it. If a column with that name does not exist, raise an error.
}

@defproc[(table-with-column [df table?] 
                            [data sequence?] 
                            [#:as as (or/c symbol? #f) #f]) 
         table?]{
 Returns a new table with either the column data added or replaced if @racket[as] is the same name as an existing column. If no column name is provided then a new column name is generated for it.

 If the @racket[data] sequence contains fewer elements than there are rows in the table, then the extra rows will be filled with @racket[#f] for the new column. Likewise, if @racket[data] contains more values than there are rows, the extra values will be dropped.

 It's important to note that the vector for the column generated @italic{will be as large as necessary for it to be indexed properly by the table!} For example, let's say you begin with a table that has 1M rows and filter it, which returns a new table with a single row. If the index of that single row is 999999 (i.e., the last row), then adding a new column will create a vector with 1M entries, all but one of which will contain @racket[#f].
}

@defproc[(table-with-columns-renamed [df table?] 
                                     [rename-map hash-eq?]) 
         table?]{
 Returns a new table with the columns in @racket[rename-map] renamed. Example:

 @racketblock[
  (table-with-columns-renamed df #hasheq((person . name)))
 ]
}

@defproc[(table-cut [df table?] 
                    [ks (non-empty-listof symbol?)]) 
         table?]{
 Returns a new table with only the columns @racket[ks].
}

@defproc[(table-drop [df table?] 
                     [ks (non-empty-listof symbol?)]) 
         table?]{
 Returns a new table with the columns @racket[ks] removed.
}

@defproc[(table-irow [df table?] 
                     [i exact-nonnegative-integer?]) 
         (listof any/c)]{
 Given an index position, return a row (list) of the values in the columns at that position. An index position is the exact index into the column data the table references. This is usually not what you want, but can be useful in some situations.

 For a more concrete example of this, imagine a brand new table with a single column of 3 value: @racket[#(a b c)]; it has an index of @racket[#(0 1 2)]. Now, reverse the table; the index is now @racket[#(2 1 0)]. Calling @racket[(table-irow df 2)] will return @racket['(c)], because that's the value at index 2. However, calling @racket[(table-row df 2)] will return @racket['(a)], because that's the value of the third row; the table has been reversed.

 This can be seen in action easily with the following code:

 @racketblock[
  (let ([df (table-reverse (table-with-column empty-table '(a b c)))])
    (for ([(i row) df] [n (in-naturals)])
      (displayln (format " for ~a = ~v" i row))
      (displayln (format "irow ~a = ~v" i (table-irow df i)))
      (displayln (format " row ~a = ~v" n (table-row df n)))))
 ]

 The for loop follows the index, so index 2 should be output first, which is reference row 0. The last index output is 0, which is the last reference row (2).
}

@defproc[(table-row [df table?] 
                    [i exact-nonnegative-integer?]) 
         (list/c any/c)]{
 Given an reference position, return a row (list) of the values in the columns at that position. A reference position is similar to @racket[vector-ref] or @racket[list-ref]: it is the zero-based, nth row within the table.

 See the comment for @racket[table-irow] for more details.
}

@defproc[(table-record [df table?] 
                       [i exact-nonnegative-integer?]) 
         hash-eq?]{
 Given an reference position, return a record (hash) of the columns and values for that row.
}

@defproc[(table-rows [df table?]) (sequenceof list?)]{
 Iterates over the table, returning a row (list) for each row. This is different from iterating over the table itself, because the table sequence also returns the index along with the row.
}

@defproc[(table-records [df table?]) (sequenceof hash-eq?)]{
 Iterates over the table, returning a record (hash) for each row.
}

@defproc[(table-head [df table?] 
                     [n exact-nonnegative-integer? 10]) 
      table?]{
 Returns a new table that is just the first @racket[n] rows of @racket[df].
}

@defproc[(table-tail [df table?] 
                     [n exact-nonnegative-integer? 10]) 
      table?]{
 Returns a new table that is just the last @racket[n] rows of @racket[df].
}

@defproc[(table-select [df table?] 
                       [flags (sequenceof any/c)]) 
      table?]{
 Given a sequence of boolean values, filters the rows of @racket[df] and returns a new table. Use @racket[table-filter] to filter using a predicate function.
}

@defproc[(table-map [df table?]
                    [proc ((non-empty-listof any/c) -> any/c)]
                    [ks (or/c (non-empty-listof symbol?) #f) #f])
      sequence?]{
 Provided an optional list of columns, calls @racket[proc] for every row in @racket[df] represented as a list. If @racket[ks] is @racket[#f] then all columns are used.

 Example:

 @racketblock[
  (define df (table-read/columns '(("Jeff" "Aaron" "Rachel")
                                   (48 14 24))
                                 '(name age)))

  (sequence->list (table-map df (λ (row) (string-join (map ~a row)))))
 ]
}

@defproc[(table-apply [df table?]
                      [proc procedure?]
                      [ks (or/c (non-empty-listof symbol?) #f) #f])
      sequence?]{
 Like @racket[table-map], but applies @racket[proc] with multiple arguments (one per column) as opposed to a single list per row. The arguments are supplied in column-order.

 Example:

 @racketblock[
  (define df (table-read/columns '(("Jeff" "Aaron" "Rachel")
                                   (48 14 24))
                                 '(name age)))

  (sequence->list (table-apply df (λ (name age) (format "~a ~a" name age))))
 ]
}

@defproc[(table-filter [df table?]
                       [proc procedure?]
                       [ks (or/c (non-empty-listof symbol?) #f) #f])
      table?]{
 Like @racket[table-apply], but the resulting sequence is used for a @racket[table-select]. A new table is returned.
}

@defproc[(table-update [df table?]
                       [k symbol?]
                       [proc procedure?]
                       [#:ignore-na? ignore-na boolean? #t])
      table?]{
 Applies the column @racket[k] to @racket[proc] and returns a new table with the column values replaced. This is similar to:

 @racketblock[(table-with-column df (table-apply df proc (list k)) #:as k)]

 If @racket[ignore-na] is @racket[#t] (the default), then all @racket[#f] values are returned as @racket[#f] instead of being updated.
}

@defproc[(table-fold [df table?]
                     [proc (any/c any/c -> any/c)]
                     [i any/c]
                     [final (any/c -> any/c) identity])
         table?]{
 Returns a table with a single row, where each column has been aggregated with @racket[proc], with an initial value of @racket[i]. Optionally, a @racket[final] function can be applied to the result before returning.
}

@defproc[(table-groupby [df table?]
                        [ks (non-empty-listof symbol?)]
                        [less-than? (or/c (any/c any/c -> boolean?) #f) sort-ascending])
         (sequence/c (listof (list/c symbol? any/c)) table?)]{
 Creates and returns a sequence of reference indices grouped by the columns in @racket[ks]. Each iteration of the sequence returns two values: an associative list of the group in @racket[(k value)] form and the subtable of all rows for that group. If @racket[less-than?] is @racket[#f] then the groups are returned in the whatever order they appeared in the source table. The rows within the resulting group subtables are stable: their order relative to each other is the same as the original table.
}

@defproc[(table-drop-na [df table?]
                        [ks (or/c (non-empty-listof symbol?) #f) #f])
      table?]{
 Returns a new table with all rows dropped that have missing values among the columns specified in @racket[ks] (or any column if @racket[ks] is @racket[#f]).
}

@defproc[(table-reverse [df table?]) table?]{
 Returns a new table with the index order reversed.
}

@defproc[(table-sort [df table?]
                     [ks (or/c (non-empty-listof symbol?) #f) #f]
                     [less-than? (any/c any/c -> boolean?) sort-ascending]) table?]{
 Returns a new table with the index of @racket[df] sorted by the columns @racket[ks] (or all columns if @racket[#f]) sorted by @racket[less-than?]. By default, it will sort in ascending order using a custom sorting predicate.
}

@defproc[(table-distinct [df table?]
                         [ks (or/c (non-empty-listof symbol?) #f) #f]
                         [keep (or/c 'first 'last 'none) 'first])
         table?]{
 Returns a new table removing duplicate rows where all the columns specified in @racket[ks] are @racket[equal?].
 When @racket[ks] is @racket[#f], all columns are compared.
}

@defproc[(table-join/inner [df table?]
                           [other table?]
                           [on (non-empty-listof symbol?)]
                           [less-than? (any/c any/c -> boolean?) sort-ascending]
                           [#:with with (non-empty-listof symbol?) on])
         table?]{
 Performs an INNER join of @racket[df] and @racket[other].

 Rows are joined using the columns of @racket[df] specified by @racket[on] with the columns of @racket[other] specified by @racket[with]. If @racket[with] is not provided, then the columns joined are expected to be the same for both tables. The columns joined must be @racket[equal?].

 Example joining two tables using the same column name:

 @racketblock[
  (table-join/inner df other '(id))
 ]

 Example joining two tables with different column names:

 @racketblock[
  (table-join/inner df other '(name gender) #:with '(surname sex))
 ]

 If the @racket[on] and @racket[with] parameters are of different lengths, no error will be triggered and an empty table will be returned as all comparisons will fail. 
}

@defproc[(table-join/outer [df table?]
                           [other table?]
                           [on (non-empty-listof symbol?)]
                           [less-than? (any/c any/c -> boolean?) sort-ascending]
                           [#:with with (non-empty-listof symbol?) on])
         table?]{
 Identical to @racket[table-join/inner], except that the join performed is a LEFT OUTER join.
}


@;; ----------------------------------------------------
@section{Printing Tables}

@defparam[pretty-print-rows rows (or/c exact-nonnegative-integer? #f) #:value 10]{
  Controls the maximum number of rows output by @racket[write-table]. If set to @racket[#f] then there is no limit and all rows will be printed.
}

@defproc[(format-table [df table?]
                       [port output-port?]
                       [mode boolean?]
                       [#:keep-index? keep-index boolean? #t])
         void?]{
 Pretty prints a maximum of @racket[pretty-print-rows] rows of @racket[df] to @racket[port].
 If the table contains more rows than this, then the head and the tail of the table are output, and intermediate rows are elided.

 If @racket[mode] is #t, the table elements are printed in the style of print. If it is false, they are printed in the style of display.

 If @racket[keep-index] is @racket[#t] then the index column is also output as well.
}

@defproc[(print-table [df table?]
                      [port output-port? (current-output-port)]
                      [#:keep-index? keep-index boolean? #t])
         void?]{
 Calls @racket[format-table] with the mode set to @racket[#t].
}

@defproc[(display-table [df table?]
                        [port output-port? (current-output-port)]
                        [#:keep-index? keep-index boolean? #t])
         void?]{
 Calls @racket[format-table] with the mode set to @racket[#f].
}


@;; ----------------------------------------------------
@section{Writing Tables}

@defproc[(table-write/string [df table?]
                             [port output-port? (current-output-port)])
         void?]{
 Pretty print the entire table to @racket[port] using @racket[display-table], temporarily setting @racket[pretty-print-rows] to @racket[#f] beforehand.
}

@defproc[(table-write/csv [df table?]
                          [port output-port? (current-output-port)]
                          [#:keep-index? keep-index boolean? #t]
                          [#:header? header boolean? #t]
                          [#:separator-char sep char? #\,]
                          [#:quote-char quote char? #\"]
                          [#:escape-char escape char? #\\]
                          [#:list-char list-sep char? #\|]
                          [#:double-quote? double-quote boolean? #t]
                          [#:na-rep na string? ""]
                          [#:na-values na-values (listof any/c) '(#f)])
         void?]{
 Outputs @racket[df] to @racket[port] in a CSV format.
}

@defproc[(table-write/json [df table?]
                           [port output-port? (current-output-port)]
                           [#:orient orient (or/c 'records 'columns) 'records]
                           [#:lines? lines boolean? #t]
                           [#:na-rep na any/c (json-null)])
         void?]{
 Outputs @racket[df] to @racket[port] in JSON. 
 
 If @racket[orient] is @racket['records] (the default) then every row of the table is written as an array of JSON objects. If @racket[lines] is @racket[#t] (and @racket[orient] is @racket['records]) then instead of an array, then each row is written as a record on each line.

 If @racket[orient] is @racket['columns] then the table is written as a single JSON object, where each key/value pairing is the column name and an array of values.

 The @racket[na] determines what Racket value in written out as a JSON null.
}


@;; ----------------------------------------------------
@section{Columns}

@defstruct[column ([name symbol?]
                   [index (vectorof exact-nonnegative-integer?)]
                   [data (vectorof any/c)])]{
 The constructor for a new column. There should almost never be a need to call this directly as opposed to having one created for you using the @racket[table-column] function, which shares the same index and data values for the table. All columns are also sequences and can be iterated using @racket[for].
}

@defthing[empty-column column?]{
 The immutable empty column.
}

@defproc[(build-column [data (sequenceof any/c)]
                       [#:as as (or/c symbol? #f) #f])
         column?]{
 Builds a new column with the values in @racket[data]. The data is copied and a new index is built for the column. If @racket[#:as] is @racket[#f] then a unique column name will be generated for it.
}

@defproc[(column-length [col column?]) exact-nonnegative-integer?]{
 Returns the number of data items referenced by the index.
}

@defproc[(column-empty? [col column?]) boolean?]{
 Returns @racket[#t] if the column's index is empty.
}

@defproc[(column-compact [col column?]) column?]{
 Returns a new column with duplicated, but (presumably) reduced data and memory usage. This is useful if the original column contains lots of data, but a very small index.
}

@defproc[(column-rename [col column?]
                        [as (or/c symbol? #f) #f]) column?]{
 Returns a new column, referencing the same data as @racket[col], but with a different name. If @racket[as] is not provided, then a unique colum name will be generated.
}

@defproc[(column-ref [col column?]
                     [n exact-nonnegative-integer?])
         any/c]{
 Returns the nth item from the indexed data in the column.
}

@defproc[(column-head [col column?]
                      [n exact-nonnegative-integer? 10])
         column?]{
 Returns a new column that shares data with @racket[col], but only contains the first @racket[n] items.
}

@defproc[(column-tail [col column?]
                      [n exact-nonnegative-integer? 10])
         column?]{
 Returns a new column that shares data with @racket[col], but only contains the last @racket[n] items.
}

@defproc[(column-reverse [col column?]) column?]{
 Returns a new column that shares data with @racket[col], but with the index reversed.
}

@defproc[(column-sort [col column?]
                      [less-than? ((any/c any/c) -> boolean?) sort-ascending]) column?]{
 Returns a new column that shares data with @racket[col], but with the index sorted by the data values.
}


@;; ----------------------------------------------------
@section{Groups}

@defproc[(group-fold [proc (any/c any/c -> any/c)]
                     [init any/c]
                     [group (sequence/c (listof (list/c symbol? any/c)) table?)]
                     [final (any/c -> any/c) identity])
         table?]{
 Iterates over every table in the group and calls @racket[table-fold] for each. The result of each fold is appended and returned in a final table of results.
}

@defproc[(group-count [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Counts every non @racket[#f] value in each group.
}

@defproc[(group-min [group (sequence/c (listof (list/c symbol? any/c)) table?)]
                    [less-than? (any/c any/c -> boolean?) sort-ascending])
         table?]{
 Returns the minimum value for each group.
}

@defproc[(group-max [group (sequence/c (listof (list/c symbol? any/c)) table?)]
                    [greater-than? (any/c any/c -> boolean?) sort-descending])
         table?]{
 Returns the maximum value for each group.
}

@defproc[(group-mean [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Sums all non @racket[#f] values and then averages them at the end. The average is of all valid values and across all rows. For example, the mean of the values @racket['(2 #f 4)] is @racket[3] not @racket[2].
}

@defproc[(group-sum [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Adds every non @racket[#f] value in each group.
}

@defproc[(group-product [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Multiplies every non @racket[#f] value in each group.
}

@defproc[(group-and [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 If every value is non @racket[#f] then the result is @racket[#t], otherwise @racket[#f].
}

@defproc[(group-or [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 If any value is non @racket[#f] then the result is @racket[#t], otherwise @racket[#f].
}

@defproc[(group-list [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Collects all non @racket[#f] values into a list.
}

@defproc[(group-unique [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Collects all non @racket[#f] values into a list, keeping only unique values.
}

@defproc[(group-nunique [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Counts all non @racket[#f], unique values. 
}

@defproc[(group-sample [group (sequence/c (listof (list/c symbol? any/c)) table?)]) table?]{
 Picks a random value among all the non @racket[#f] values. This uses @hyperlink["https://en.wikipedia.org/wiki/Reservoir_sampling"]{Reservoir Sampling} to ensure the selection is fair and works for arbitrarily large sets. 
}


@;; ----------------------------------------------------
@section{Indexes}

@defstruct[index ([keys (vectorof (cons/c any/c (vectorof exact-nonnegative-integer?)))]
                  [less-than? (or/c ((any/c any/c) -> boolean?) #f)])]{
 Constructor for a new index. The @racket[keys] are a sorted vector of lists, where the first element of the list is the key value and the rest of the list are indices. The @racket[less-than?] predicate is the same as was used to sort @racket[keys] before passing them in.
}

@defproc[(build-index [data (sequenceof any/c)]
                      [less-than? (or/c ((any/c any/c) -> boolean?) #f) sort-ascending])
         index?]{
 Creates a new index by finding all the unique keys in the @racket[data] sequence along with the ordered indices where they keys are located, then sorts them using the @racket[less-than?] predicate if defined. If the @racket[less-than?] predicate function is @racket[#f] then no sorting takes place and the keys are in a random order.
}

@defthing[empty-index index?]{
 An empty index.
}

@defproc[(index-scan-keys [ix index?]
                          [#:from from any/c #f]
                          [#:to to any/c #f])
         sequence?]{
 Returns a sequence from the @racket[from] key (inclusive) to the @racket[to] key (exclusive). If @racket[from] is @racket[#f] then the sequence begins with the first key. If @racket[to] is @racket[#f] then the sequence ends with the last key in the index. The sequence returns multiple values: the key and a list of all the reference indices of the data sequence where the keys originated from.
 
 If the index is not sorted then the order of the sequence returned is undefined.
}

@defproc[(index-scan [ix index?]
                     [#:from from any/c #f]
                     [#:to to any/c #f])
         sequence?]{
 Similar to @racket[index-scan-keys], but instead of the sequence returning multiple values, this sequence only returns the indices, in order.
}

@defproc[(index-length [ix index?]) exact-nonnegative-integer?]{
 Returns the number of unique keys in the index.
}

@defproc[(index-empty? [ix index?]) boolean?]{
 True if the index has no keys.
}

@defproc[(index-sorted? [ix index?]) boolean?]{
 True if the index was defined with a less-than? predicate. 
}

@defproc[(index-find [ix index?]
                     [key any/c]
                     [exact boolean? #f])
         (or/c exact-nonnegative-integer? #f)]{
 Searches the index looking for a matching @racket[key]. If the index is sorted then this is a binary search, otherwise it's a linear search through all the keys for a match. 

 If @racket[key] is found, then the reference index to the keys is returned. When not found, if @racket[exact] is @racket[#t] then @racket[#f] is returned. Otherwise, the next higher index for the next key is returned.
}

@defproc[(index-member [ix index?]
                       [key any/c])
         (or/c (list/c any/c exact-nonnegative-integer? ...) #f)]{
 Searches the index looking for an exactly matching @racket[key]. If found then the list of key and indices is returned, otherwise @racket[#f] is returned.
}

@defproc[(index-ref [ix index?]
                    [n exact-nonnegative-integer?])
         (list/c any/c exact-nonnegative-integer? ...)]{
 Returns the key and indices at the given reference index.
}

@defproc[(index-map [ix index?]
                    [v (vectorof any/c)]
                    [#:from from any/c #f]
                    [#:to to any/c #f])
         sequence?]{
 Scans the index and maps the indices across the values in @racket[v] and returns them in a new sequence.
}

@defproc[(index-min [ix index?]) (or/c any/c #f)]{
 Returns @racket[#f] if the index is empty, otherwise returns the first key in the index.
}

@defproc[(index-max [ix index?]) (or/c any/c #f)]{
 Returns @racket[#f] if the index is empty, otherwise returns the last key in the index.
}

@defproc[(index-median [ix index?]) (or/c any/c #f)]{
 Returns @racket[#f] if the index is empty, otherwise returns the median key in the index.
}

@defproc[(index-mode [ix index?]) (or/c any/c #f)]{
 Returns @racket[#f] if the index is empty, otherwise returns the key that occurs the most often.
}


@;; ----------------------------------------------------
@section{Sort Ordering}

All functions that allow for sorting (e.g. @racket[table-sort]) or indexing/grouping take an optional less-than? compare function. Tabular Asa comes with a generic orderable interface with @racket[sort-ascending] and @racket[sort-descending] functions for the following basic types:

@itemlist[
 @item{Boolean}
 @item{Number}
 @item{String}
 @item{Char}
 @item{Symbol}
 @item{Sequence}
 @item{Date}
]

Both generic functions will always sort @racket[#f] values last regardless of sort direction.

@defproc[(sort-ascending [a orderable?] [b any/c]) boolean?]{
 Returns @racket[#t] if @racket[b] is @racket[#f] or @racket[a] < @racket[b].
}

@defproc[(sort-descending [a orderable?] [b any/c]) boolean?]{
 Returns @racket[#t] if @racket[b] is @racket[#f] or @racket[a] > @racket[b].
}

@defproc[(orderable? [x any/c]) boolean?]{
 Returns @racket[#t] if @racket[x] is a of a type that can ordered using @racket[sort-ascending] or @racket[sort-descending].
}