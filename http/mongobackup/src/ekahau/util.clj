(ns ekahau.util
  (:require
   clojure.walk)
  (:use
    [clojure.set :only (union select)]
    [clojure.contrib.duck-streams :only (reader)]
    [clojure.contrib.str-utils :only (str-join)])
  (:import
    [java.io File Reader]
    [java.util Properties]
    [javax.imageio ImageIO ImageReader]
    [javax.imageio.stream ImageInputStream]
    [com.csvreader CsvReader CsvWriter]))

;
; Printing helpers
;

(defn- print-rows
  [x]
  (print (str (str-join "\n" x)) "\n"))

(defn mac-to-long
  [s]
  (Long/parseLong (apply str (remove #{\:} s)) 16))

(defn long-to-mac
  [val]
  (let [s (Long/toHexString val)]
    (apply str (apply concat (interpose '(\:) (partition 2 (concat (repeat (- 12 (count s)) \0) s)))))))

(defn create-ids
  [count starting-from]
  (take count (iterate inc starting-from)))

(defn enumerate-names
  [prefix]
  (map #(str prefix " " %) (iterate inc 1)))

(defn get-image-dimensions
  [^String filename]
  (when-let [^ImageInputStream in (ImageIO/createImageInputStream (File. filename))]
    (try
      (let [readers (iterator-seq (ImageIO/getImageReaders in))
            ^ImageReader reader (doto ^ImageReader (first readers) (.setInput in))
            dispose-readers (fn [] (doseq [^ImageReader reader readers] (.dispose reader)))]
        (try
          (vector (.getWidth reader 0) (.getHeight reader 0))
          (finally
            (dispose-readers))))
      (finally (.close in)))))

(defn- capitalize
  "Uppercase the first letter of a string, and lowercase the rest."
  [^String s]
  (if (< 0 (.length s))
    (str (.toUpperCase (subs s 0 1))
      (.toLowerCase (subs s 1)))
    ""))

(defn clojure-to-camel
  [^String s]
  (let [parts (.split s "-")]
    (apply str (first parts) (map #(capitalize %) (rest parts)))))

(defn camel-to-clojure
  [s]
  (let [parts (re-seq #"(\p{L}\p{Ll}*)|(\d+)" s)]
    (str-join "-" (map #(.toLowerCase ^String (first %)) parts))))

(defn- transform-keyword
  [kw f]
  (keyword (f (name kw))))

(defn clojure-keyword-to-camel
  "Returns a keyword with name converted to camel case."
  [kw]
  (transform-keyword kw clojure-to-camel))

(defn camel-keyword-to-clojure
  "Returns a keyword with name converted from camel case to hyphenated"
  [kw]
  (transform-keyword kw camel-to-clojure))

(defn transform-map-keys
  [m f]
  (into {} (map (juxt (comp f key) val) m)))

(defn convert-keys-to-camel-case
  "Converts map keys to camel case."
  [m]
  (transform-map-keys m clojure-keyword-to-camel))

(defn convert-keys-from-camel-case
  "Converts map keys from came case to hyphenated."
  [m]
  (transform-map-keys m camel-keyword-to-clojure))

(defn transform-tree-keys
  "Recursively transforms all map keys from keywords to strings."
  [m f]
  (let [f (fn [[k v]] (if (keyword? k) [(f k) v] [k v]))]
    ;; only apply to maps
    (clojure.walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn keys-recursively-to-clojure
  [m]
  (transform-tree-keys m camel-keyword-to-clojure))

(defn keys-recursively-to-camel-case
  [m]
  (transform-tree-keys m clojure-keyword-to-camel))

(defn create-temp-dir
  [^File base-dir prefix]
  (loop [num (System/currentTimeMillis)]
    (let [d (File. base-dir (str prefix num))
          is-new (.mkdirs d)]
      (if is-new
        d
        (recur (inc num))))))

(defmacro with-temp-dir
  [name base-dir & body]
  `(let [~name (create-temp-dir ~base-dir "temp-")]
     (try
       ~@body
       (finally
         (.delete ~name)))))

(defn delete-matching-files
  [^File dir pred]
  (doseq [^String filename (filter pred (seq (.list dir)))]
    (.. (File. dir filename) delete)))

(defn ends-with?
  [^String name & endings]
  (some #(.endsWith name %) endings))

(defn create-files-in
  [^File dir names]
  (doseq [^String name names] (.. (File. dir name) (createNewFile))))

(defn create-properties-from-map
  [m]
  (let [properties (Properties.)]
    (doseq [[k v] m]
      (.setProperty properties k v))
    properties))

;; Misc ;;

(defn- remove-nil-value-entries
  [keyvals]
  (when-not (even? (count keyvals))
    (throw (IllegalArgumentException. "Even number of arguments required")))
  (apply concat (remove (comp nil? second) (partition 2 keyvals))))

(defn hash-map-with-non-nil-values
  [& keyvals]
  (apply hash-map (remove-nil-value-entries keyvals)))

(defn- filter-keyvals
  [pred keyvals]
  (->> keyvals
    (partition 2)
    (filter (comp pred second))
    (apply concat)))

(defn assoc-if
  [m pred & keyvals]
  (if-let [kvs (seq (filter-keyvals pred keyvals))]
    (apply assoc m kvs)
    m))

(defn dissoc-if
  [m pred & ks]
  (apply dissoc m (filter #(pred (get m %)) ks)))

(defn assoc-multi
  [m k v default]
  (assoc m k (conj (get m k default) v)))

(defn assoc-multi-kv
  ([m kv]
    (assoc-multi-kv m kv nil))
  ([m default [k v]]
    (assoc-multi m k v default)))

(defn- get-keys-of-matching-vals
  [m val-pred]
  (map key (filter (comp val-pred val) m)))

(defn- select-keys-if
  [m pred]
  (select-keys m (get-keys-of-matching-vals m pred)))

(defn select-keys-with-non-nil-values
  [m]
  (select-keys-if m (complement nil?)))

(defn get-descendants
  [elem get-children]
  (let [elem-children (get-children elem)]
    (apply union (into #{} elem-children) (map #(get-descendants % get-children) elem-children))))

(defn update-values
  [m & keyvalfns]
  (reduce
    (fn [m [k f]] (update-in m [k] f))
    m (partition 2 keyvalfns)))

(defn partition-by-counts
  "Partitions coll to chunks, sizes of partitions determined by counts."
  [counts coll]
  (when (and (seq counts) (seq coll))
    (lazy-seq
      (let [[part coll-rest] (split-at (first counts) coll)]
        (cons part (partition-by-counts (rest counts) coll-rest))))))

(defn has-id? 
  [v id]
  (= (:id v) id))

(defn select-by-id 
  [xset id]
  (select #(has-id? % id) xset))

(defn- next-free-id
  [coll]
  (apply max 0 (map inc (remove nil? (map :id coll)))))

(defn assign-ids
  "Returns a sequence of based on coll where each item that does not have an :id gets one."
  ([coll]
    (assign-ids coll (iterate inc (next-free-id coll))))
  ([coll ids]
    (when (seq coll)
      (lazy-seq
        (let [x (first coll)]
          (if (nil? (:id x))
            (cons
              (assoc x :id (first ids))
              (assign-ids (rest coll) (rest ids)))
            (cons
              x
              (assign-ids (rest coll) ids))))))))

(defn key-from-property-path
  [path]
  (vec (map :id path)))

;; Structs ;;

(defmacro struct-map-fn
  "Creates a struct initializer function initializes keys in ks."
  {:private true}
  [s & ks]
  (let [value-symbols (take (count ks) (repeatedly #(gensym "VAL")))
        value-symbol-vector (vec value-symbols)
        struct-map-inits (interleave ks value-symbols)]
    `(fn ~value-symbol-vector
       (struct-map ~s ~@struct-map-inits))))

(defn structs-from-rows
  "Creates struct-map instances initialized with values in rows for keys."
  [s col-keys & rows]
  (letfn [(row-fn [values] (apply struct-map s (interleave col-keys values)))]
    (map row-fn rows)))

;; CSV ;;

(defn- csv-record-seq
  [^CsvReader csvreader]
  (lazy-seq
    (if (.readRecord csvreader)
      (cons (seq (.getValues csvreader)) (csv-record-seq csvreader))
      nil)))

(defn parse-csv
  [input]
  (with-open [csvreader (CsvReader. ^Reader (reader input))]
    (doall (csv-record-seq csvreader))))

(defn write-csv
  ([file-name entities fields]
    (with-open [csvw (CsvWriter. file-name)]
      (let [csvwfn #(.writeRecord csvw (into-array String (map str %)) true)]
        (csvwfn (map name fields))
        (doseq [e entities]
          (csvwfn (map #(get e %) fields))))
      file-name))
  ([entities fields]
    (write-csv (.getCanonicalPath
                 (doto (File/createTempFile "entities" ".csv")
                   (.deleteOnExit)))
      entities fields)))

;; ns utils ;;

(defn get-multifns
  [nsname]
  (map :name
    (filter #(= (:tag %) clojure.lang.MultiFn)
      (map meta
        (vals (ns-publics nsname))))))

;; filter search ;;

(def search-fn-to-pred-fn
  ^{:private true}
  {"IN"          (fn [es1 e] (contains? es1 e))
   "NOT-IN"      (fn [es1 e] (not (contains? es1 e)))
   "SOME-IN"     (fn [es1 es] (some #(contains? es1 %) es))
   "SOME-KEY-IN" (fn [es1 eks es] (some #(contains? es1 %)
                                    (map (apply comp (reverse eks)) es)))
   "="           (fn [e1 e] (= e e1))
   "SOME="       (fn [e1 es] (some #(= e1 %) es))
   "SOME-KEY="   (fn [e1 eks es] (some #(= e1 %)
                                   (map (apply comp (reverse eks)) es)))
   "<="          (fn [e1 e] (<= (compare e e1) 0))
   ">="          (fn [e1 e] (>= (compare e e1) 0))
   "<"           (fn [e1 e] (< (compare e e1) 0))
   ">"           (fn [e1 e] (> (compare e e1) 0))})

(defn filter-search
  "A search param is [f ks vs] which is converted into (f vs #(get-in % ks)) 
   1) f is the search fn in the above map
   2) ks is converted to #(get-in % ks)
   3) vs is the value(s) against which to match
      Note that vs is a coll if f is IN, NOT-IN, SOME-IN, SOME-KEY-IN
      Else vs is a simple value
   4) The above works for simple searches. But for more complex searches you can
      provide a pred-fn g of your own i.e (g e) is used to filter out.
   5) Filter search will check if the search param is a vector?,
      if it is, it will do 1. above, if not it will assume that you
      have passed a pred-fn g and directly use that.
   egs (>= 10312312 (#(get-in % [:timestamp]) e))
       (IN [5 6 7] (#(get-in % [:position-observation :position :zone :id]) e))"
  [search-params entities]
  (if (seq search-params)
    (let [pred (apply juxt
                 (map (fn [sp]
                        (if (vector? sp)
                          (let [[^String f ks vs] sp]
                            (if-let [pred-fn (get search-fn-to-pred-fn f)]
                              (let [pred-fn (partial pred-fn
                                              (if (.contains f "IN")
                                                (set vs) vs))
                                    [ks pred-fn] (if (.startsWith f "SOME-KEY")
                                                   (let [[ks eks] ks]
                                                     [ks (partial pred-fn eks)])
                                                   [ks pred-fn])]
                                (fn [e]
                                  (pred-fn (get-in e ks))))
                              (throw (Exception. (str "no corresponding pred fn for search fn " f)))))
                          sp))
                   search-params))]
      (filter (fn [e]
                (reduce #(and %1 %2) (pred e)))
        entities))
    entities))
