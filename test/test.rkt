#lang racket

#|

Tabular Asa - a fast, efficient, dataframes implementation

Copyright (c) 2021 by Jeffrey Massung
All rights reserved.

|#

(require rackunit)

;; ----------------------------------------------------

(require "../main.rkt")

;; ----------------------------------------------------

(table-preview display-table)

;; ----------------------------------------------------

(define (sequence-equal? xs ys)
  (equal? (sequence->list xs)
          (sequence->list ys)))

;; ----------------------------------------------------

(define (check-table df header rows)
  (check-true (and (equal? (table-header df) header)
                   (sequence-equal? (table-rows df) rows))))

;; ----------------------------------------------------

(define heroes (call-with-input-file "heroes.csv" table-read/csv))

;; ----------------------------------------------------

(test-case "Loading CSV"
           (check-table heroes
                        '(hero year universe)
                        '(("Superman" 1938 "DC")
                          ("Batman" 1939 "DC")
                          ("Flash" 1940 "DC")
                          ("Captain America" 1941 "Marvel")
                          ("Wonder Woman" 1941 "DC")
                          ("Fantastic Four" 1961 "Marvel")
                          ("X-Men" 1963 "Marvel"))))

;; ----------------------------------------------------

(define hero-creators (call-with-input-file "creators.json" table-read/json))

;; ----------------------------------------------------

(test-case "Reading JSON"
           (check-table hero-creators
                        '(creator hero)
                        '(("Stan Lee" "Fantastic Four")
                          ("Joe Simon" "Captain America")
                          ("Bob Kane" "Batman")
                          ("Todd McFarlane" "Spawn")
                          ("Jerry Siegel" "Superman")
                          ("William Marston" "Wonder Woman")
                          ("Stan Lee" "X-Men")
                          ("Gardner Fox" "Flash"))))

;; ----------------------------------------------------

(define marvel-heroes (table-filter heroes (λ (uni) (string=? uni "Marvel")) '(universe)))

;; ----------------------------------------------------

(test-case "Test filtering"
           (check-table marvel-heroes
                        '(hero year universe)
                        '(("Captain America" 1941 "Marvel")
                          ("Fantastic Four" 1961 "Marvel")
                          ("X-Men" 1963 "Marvel"))))

;; ----------------------------------------------------

(define sorted-heroes (table-sort marvel-heroes '(year) sort-descending))

;; ----------------------------------------------------

(test-case "Test sorting post-filter"
           (check-table sorted-heroes
                        '(hero year universe)
                        '(("X-Men" 1963 "Marvel")
                          ("Fantastic Four" 1961 "Marvel")
                          ("Captain America" 1941 "Marvel"))))

;; ----------------------------------------------------

(test-case "Test head/tail"
           (check-table (table-head sorted-heroes 2)
                        '(hero year universe)
                        '(("X-Men" 1963 "Marvel")
                          ("Fantastic Four" 1961 "Marvel")))
           (check-table (table-tail sorted-heroes 2)
                        '(hero year universe)
                        '(("Fantastic Four" 1961 "Marvel")
                          ("Captain America" 1941 "Marvel"))))

;; ----------------------------------------------------

(define heroes-w/creator (table-join/inner heroes hero-creators '(hero)))

;; ----------------------------------------------------

(test-case "Test inner join"
           (check-table heroes-w/creator
                        '(hero year universe creator)
                        '(("Superman" 1938 "DC" "Jerry Siegel")
                          ("Batman" 1939 "DC" "Bob Kane")
                          ("Flash" 1940 "DC" "Gardner Fox")
                          ("Captain America" 1941 "Marvel" "Joe Simon")
                          ("Wonder Woman" 1941 "DC" "William Marston")
                          ("Fantastic Four" 1961 "Marvel" "Stan Lee")
                          ("X-Men" 1963 "Marvel" "Stan Lee"))))

;; ----------------------------------------------------

(test-case "Test distinct"
           (check-table (table-distinct heroes-w/creator '(universe) 'last)
                        '(hero year universe creator)
                        '(("Wonder Woman" 1941 "DC" "William Marston")
                          ("X-Men" 1963 "Marvel" "Stan Lee"))))

;; ----------------------------------------------------

(define creators-w/hero (table-join/outer hero-creators heroes '(hero)))

;; ----------------------------------------------------

(test-case "Test outer join"
           (check-table creators-w/hero
                        '(creator hero year universe)
                        '(("Stan Lee" "Fantastic Four" 1961 "Marvel")
                          ("Joe Simon" "Captain America" 1941 "Marvel")
                          ("Bob Kane" "Batman" 1939 "DC")
                          ("Todd McFarlane" "Spawn" #f #f)
                          ("Jerry Siegel" "Superman" 1938 "DC")
                          ("William Marston" "Wonder Woman" 1941 "DC")
                          ("Stan Lee" "X-Men" 1963 "Marvel")
                          ("Gardner Fox" "Flash" 1940 "DC"))))

;; ----------------------------------------------------

(define hero-universe (table-cut creators-w/hero '(hero universe)))
(define grouped-heroes (table-groupby hero-universe '(universe)))

;; ----------------------------------------------------

(test-case "Count heroes by group"
           (check-table (group-count grouped-heroes)
                        '(universe hero)
                        '(("DC" 4)
                          ("Marvel" 3))))

;; ----------------------------------------------------

(test-case "Last hero by group"
           (check-table (group-max grouped-heroes)
                        '(universe hero)
                        '(("DC" "Wonder Woman")
                          ("Marvel" "X-Men"))))

;; ----------------------------------------------------

(test-case "Hero list by group"
           (check-table (group-list grouped-heroes)
                        '(universe hero)
                        '(("DC" ("Flash" "Wonder Woman" "Superman" "Batman"))
                          ("Marvel" ("X-Men" "Captain America" "Fantastic Four")))))

;; ----------------------------------------------------

(test-case "Sample group + join"
           (let* ([df (group-sample grouped-heroes)]

                  ; merge to get -x and -y universe per hero to ensure valid
                  [merged (table-join/inner df hero-universe '(hero))])

             ; ensure the merge renamed duplicate columns
             (check-equal? (table-header merged) '(universe-x hero universe-y))

             ; ensure each row has the same universe for the heroes
             (check-true (for/and ([(i row) merged])
                           (equal? (first row) (third row))))))

;; ----------------------------------------------------

(let* ([df (table-cut heroes '(hero universe))]

       ;
       ; NOTE: table is cut because reading JSON orders the headers alphabetical!
       ;

       [check-write/read (λ (writer reader)
                           (let* ([s (call-with-output-string writer)]
                                  [table (call-with-input-string s reader)])
                             (check-table table (table-header df) (table-rows df))))])

  (test-case "Writing CSV with index"
             (check-write/read (λ (port) (table-write/csv df port #:keep-index? #t))
                               (λ (port) (table-read/csv port #:drop-index? #t))))

  (test-case "Writing CSV without index"
             (check-write/read (λ (port) (table-write/csv df port #:keep-index? #f))
                               (λ (port) (table-read/csv port #:drop-index? #f))))

  (test-case "Writing JSON columns"
             (check-write/read (λ (port) (table-write/json df port #:orient 'columns))
                               (λ (port) (table-read/json port))))

  (test-case "Writing JSON records"
             (check-write/read (λ (port) (table-write/json df port #:orient 'records))
                               (λ (port) (table-read/json port))))

  (test-case "Writing JSON lines"
             (check-write/read (λ (port) (table-write/json df port #:lines? #t))
                               (λ (port) (table-read/json port #:lines? #t)))))
