(ns ekahau.string
  (:require
    [ekahau.UID :as uid])
  (:use
    [clojure.contrib.str-utils :only [re-split]])
  (:import
    [java.io StringWriter PrintWriter]))

(defn pluralize
  [s]
  (str s "s"))

(defn pluralize-keyword
  [k]
  (keyword (pluralize (name k))))

(defn parse-boolean
  [^String s]
  (when s
    (condp = (.toLowerCase s)
      "true"  true
      "false" false
      (throw (IllegalArgumentException. (str "Not boolean: " s))))))

(defn parse-int
  [s]
  (when s
    (Integer/parseInt s)))

(defn parse-long
  [s]
  (when s
    (Long/parseLong s)))

; Required in 1.2
(defn clojure-number
  [l]
  (when l
    (if (<= Integer/MIN_VALUE l Integer/MAX_VALUE)
      (int l)
      l)))

(defn parse-number
  [s]
  (when s
    (try
      (clojure-number (Long/parseLong s))
      (catch NumberFormatException e
        nil))))

(defn parse-double
  [s]
  (when s
    (Double/parseDouble s)))

(defn- parse-float
  [s]
  (when s
    (Float/parseFloat s)))

(def parse-id uid/parse-id)

(defn parse-calendar-day
  [^String s]
  (when s
    (let [parts (.split s "-")]
      (when (not= 3 (count parts))
        (throw (IllegalArgumentException. (str "Not a calendar day:" s))))
      (vec (map #(Integer/parseInt %) parts)))))

(defn- substrings-of-length
  [^String s len]
  (let [last-index (- (count s) len)]
    (for [index (iterate inc 0) :while (<= index last-index)]
      (.substring s index (+ index len)))))

(defn- substrings
  [s]
  (apply concat
    (for [substring-size (iterate dec (count s)) :while (< 0 substring-size)]
      (substrings-of-length s substring-size))))

(defn- string->id-seq
  [s]
  (->> s
    (re-split #",")
    (map parse-id)))

(defn string->id-set
  [s]
  (when s
    (set (string->id-seq s))))

(defn truncate-str
  ([^String s max-length]
    (truncate-str s max-length "..."))
  ([^String s max-length ^String ellipsis]
    (truncate-str s max-length ellipsis 0))
  ([^String s max-length ^String ellipsis offset]
    (if (and (> (.length s) max-length) (< (.length ellipsis) max-length))
      (str (.substring s 0 (+ (- max-length (.length ellipsis)) offset)) ellipsis)
      s)))

(defn substring-re-pattern
  [s]
  (re-pattern (str "(?i).*" s ".*")))

(defn exception->str
  [^Exception e]
  (let [s (StringWriter.)]
    (.printStackTrace e (PrintWriter. s))
    (str s)))
