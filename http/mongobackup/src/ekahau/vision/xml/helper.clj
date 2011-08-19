(ns ekahau.vision.xml.helper
  (:use
    [ekahau.util :only [convert-keys-to-camel-case hash-map-with-non-nil-values]]))

(defn get-xml-attributes
  "Converts a clojure map to prxml style attributes."
  ([m ks]
    (get-xml-attributes m ks {}))
  ([m ks transformers]
    (convert-keys-to-camel-case
      (select-keys (reduce (fn [m [k v]] (update-in m [k] v)) m transformers) ks))))

(defn get-first-child-element
  [xml tag]
  (->> (:content xml)
    (filter #(= tag (:tag %)))
    (first)))
