(ns ekahau.xml
  (:use
    [clojure.xml :only (parse)]
    [clojure.contrib.prxml :only (prxml)]
    [ekahau.util :only (clojure-keyword-to-camel
                        convert-keys-to-camel-case)]))

(defn get-elements-of-type 
  [xml-content type]
  (filter #(-> % :tag (= type)) xml-content))

(def xml-header [:decl! {:version "1.0"}])

(defn to-xml-str [root]
  "Prints an XML data structure using clojure.contrib.prxml."
  (when root
    (with-out-str
      (prxml xml-header)
      (println)
      (prxml root)
      (println))))

(defn xml-element-to-str
  [element]
  (with-out-str (prxml element)))

(defmulti convert-xml
  "Converts a data structure from clojure.xml/parse to a
  data structure suitable for clojure.contrib.prxml."
  class)

(defmethod convert-xml clojure.lang.IPersistentMap
  [element]
  (into
    [(:tag element)]
    (concat
      (if-let [attrs (:attrs element)]
        (vector attrs)
        [])
      (map convert-xml (:content element)))))

(defmethod convert-xml [class []]
  [elements]
  (vec (map convert-xml elements)))

(defmethod convert-xml String [s] s)

(defn parse-xml-string
  "Parses an XML string using clojure.xml/parse."
  [^String s]
  (with-open [in (java.io.ByteArrayInputStream. (.getBytes s "UTF-8"))]
    (parse in)))

(defn parse-prxml-string
  "Parses an XML string and returns a data structure suitable for clojure.contrib.prxml."
  [s]
  (convert-xml (parse-xml-string s)))

(defn convert-to-clojure-xml
  [pr-xml]
  (parse-xml-string (to-xml-str pr-xml)))

(def sort-by-tag-and-id
  (partial sort-by (juxt :tag (comp :id :attrs))))

(defn sort-xml-tree
  [root]
  (if (string? root)
    root
    (let [subtrees (:content root)
          sorted-subtrees (map sort-xml-tree subtrees)]
      (assoc root :content (vec (sort-by-tag-and-id sorted-subtrees))))))

(defn entity->prxml-structure
  [entity]
  (cond
    (map? entity) 
    (let [[attrs content] 
          (reduce (fn [[a c] [k v]]
                    (if (coll? v)
                      [a (conj c [k v])]
                      [(conj a [k v]) c])) 
            [[] []] entity)
          attrs (convert-keys-to-camel-case
                  (into {} attrs))]
      (into 
        [(clojure-keyword-to-camel 
           (or (:type entity) :Unknown))
         attrs]
        (map (fn [[k v]]
               [(clojure-keyword-to-camel k)
                (entity->prxml-structure v)])
          content)))
    (coll? entity)
    (map entity->prxml-structure entity)
    :else entity))

(defn entities->prxml-structure
  [type-of-entities-kw entities]
  (into [(clojure-keyword-to-camel type-of-entities-kw)]
    (map entity->prxml-structure entities)))

; remove nil from attrs coz it confuses lazy-xml
(defn remove-nil-from-attrs
  [root]
  (let [attrs (into {} (map (fn [[k v]] [k (if v v "null")]) (:attrs root)))
        content (vec (map remove-nil-from-attrs (:content root)))]
    (-> root
      (assoc :attrs attrs)
      (assoc :content content))))

