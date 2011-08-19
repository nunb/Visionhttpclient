(ns ekahau.vision.xml.zone-grouping
  (:require
    [clojure.contrib.str-utils :as str]
    [ekahau.vision.database :as database])
  (:use
    [ekahau.string :only [parse-id parse-number]]))

(defn- zone-grouping->uri
  [zone-grouping]
  (str "zoneGroupings/" (:id zone-grouping)))

(defn- zone-group->xml
  [zone-group]
  [:zoneGroup (-> zone-group
                (select-keys [:id :name :color])
                (assoc :zoneIds (str/str-join "," (sort (:zone-ids zone-group)))))])

(defn zone-grouping->xml
  [zone-grouping]
  (into
    [:zoneGrouping (-> zone-grouping
                     (select-keys [:id :name :version])
                     (assoc :uri (zone-grouping->uri zone-grouping)))]
    (map zone-group->xml (:groups zone-grouping))))

(defn zone-groupings->xml
  [zone-groupings]
  (into [:zoneGroupings]
    (map zone-grouping->xml zone-groupings)))

(defn- parse-id-list
  [^String s]
  (map parse-id (.split s ",")))

(defn xml->zone-group
  [xml]
  (let [{{:keys [name zoneIds color]} :attrs} xml]
    (struct-map database/zone-group
      :name name
      :color color
      :zone-ids (and zoneIds (seq (set (parse-id-list zoneIds)))))))

(defn- xml-elements->zone-groups
  [coll]
  (map (fn [group id] (assoc group :id id)) (map xml->zone-group coll) (iterate inc 0)))

(defn xml->zone-grouping
  [xml]
  (let [{{name :name version :version} :attrs content :content} xml
        groups (xml-elements->zone-groups content)]
    (struct-map database/zone-grouping :name name :groups groups :version (parse-number version))))
