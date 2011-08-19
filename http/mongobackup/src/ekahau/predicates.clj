(ns ekahau.predicates
  (:require
    ekahau.string)
  (:use
    [clojure.contrib.trace :only [trace]]))

(defn every-pred?
  "Mimics AND"
  [& preds]
  (fn [& args] (every? #(apply % args) preds)))

(defn any-pred?
  "Mimics OR"
  [& preds]
  (fn [& args] (some #(apply % args) preds)))

(defn value-by-key-in-set-pred?
  [k value-set]
  (fn [item]
    (boolean (value-set (get item k)))))

(defn- val-of-key-pred?
  [k pred v]
  (fn [x]
    (pred (get x k) v)))

(defn id-pred?
  [id]
  (val-of-key-pred? :id = id))

(defn substring-pred?
  [s]
  (every-pred?
    (comp not nil?)
    (partial re-matches (ekahau.string/substring-re-pattern s))))
