(ns ekahau.seq-utils)

(defn- elements-per-predicate
  [coll predicates]
  (map #(filter % coll) predicates))

(defn count-per-predicate
  [coll predicates]
  (map count (elements-per-predicate coll predicates)))
