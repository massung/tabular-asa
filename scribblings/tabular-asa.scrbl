#lang scribble/manual

@require[@for-label[tabular-asa]]

@title{Tabular Asa}
@author[@author+email["Jeffrey Massung" "massung@gmail.com"]]

@defmodule[tabular-asa]

A fast, efficient, dataframes implementation.


@;; ----------------------------------------------------
@section{Sources}

The source code can be found at @url{https://github.com/massung/tabular-asa}.


@;; ----------------------------------------------------
@section{Introduction}

Tabular Asa is intended to fulfill the following goals:

@itemlist[
 @item{Be as efficient as possible with respect to memory and performance.}
 @item{Be as lazy as possible; use sequences and streams.}
 @item{Be usable for everyday tabular data tasks.}
 @item{Be extremely simple to understand and extend.}
 @item{Be flexible by exposing easy, low-level functionality.}
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

TODO: expand further...


@;; ----------------------------------------------------
@section{Example}

@racketblock[
(define books (call-with-input-file "test/books.csv" table-read/csv))

; remove any books with an unknown author
(define known-books (table-filter (λ (i author) author) books '(Author)))

; index the table by publisher
(define ix (build-index (table-column known-books 'Publisher)))

; 
]


@;; ----------------------------------------------------
@section{Rows or Records vs. Columns}

When thinking about tabular data, it's very common to think of each row (or record) as a thing to be grouped together. However, this is extremely inefficient for most operations.


@;; ----------------------------------------------------
@section{Loading Tables}

@defproc[(table-read/csv [port input-port?]
                         [#:header? header boolean? #t]
                         [#:separator-char sep char? #\,]
                         [#:newline newline 'lax]
                         [#:quote-char quote char? #\"]
                         [#:double-quote? double-quote char? #t]
                         [#:comment-char comment char? #\#]
                         [#:strip? strip boolean? #f]
                         [#:na na any #f]
                         [#:na-values na-values list? (list "" "." "na" "n/a" "nan" "null")])
         table?]{
}

@defproc[(table-read/json [port input-port?]
                          [#:lines? lines boolean? #f])
         table?]{
}
