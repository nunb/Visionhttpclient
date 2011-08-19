(ns ekahau.vision.xml.status-type
  (:use
    [ekahau.vision.database :only [status-type status-value]]
    [ekahau.string :only [parse-id]]
    [clojure.contrib.str-utils :only [str-join]]
    [clojure.contrib.str-utils2 :only [split]]))

(defn- color-to-xml-str
  [x]
  (str-join "," x))

(defn- color-from-xml-str
  [s]
  (map #(Integer/parseInt %) (split s #",")))

(defn status-value-to-xml
  [x]
  [:value (-> x
            (update-in [:color] color-to-xml-str)
            (select-keys [:key :label :color]))])

(defn status-type-to-xml
  [x]
  (into
    [:statusType (select-keys x [:id :name])]
    (map status-value-to-xml (:values x))))

(defn- status-value-from-xml
  [xml]
  (let [attrs (:attrs xml)]
    (struct-map status-value
      :key (:key attrs)
      :label (:label attrs)
      :color (-> attrs :color color-from-xml-str))))

(defn status-type-from-xml
  [xml]
  (let [attrs (:attrs xml)]
    (struct-map status-type
      :id (-> attrs :id parse-id)
      :name (:name attrs)
      :values (map status-value-from-xml (:content xml)))))
