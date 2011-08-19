(ns ekahau.vision.xml.report
  (:require
    [ekahau.vision.database :as database])
  (:use
    [clojure.contrib.str-utils :only [str-join]]
    [ekahau.vision.xml.zone-grouping :only [zone-groupings->xml]]))

(defn create-data-key-string
  [[a b c]]
  (str "/" (str-join "/" (replace {nil "none"} a)) ";" b ";"
    "/"
    (if (nil? c)
      "none/none"
      (str-join "/" (replace {nil "none"} c)))))

(defn- get-maps-of-building-sorted-by-floors
  [db building-id]
  (->> (database/get-entities db :floors)
    (filter #(= building-id (:building-id %)))
    (sort-by :order-num)
    (map #(database/get-entity-by-id db :maps (:map-id %)))))

(defn- get-maps-without-building
  [db]
  (let [floors (database/get-entities db :floors)
        map-ids (set (map :map-id floors))]
    (filter #(not (map-ids (:id %))) (database/get-entities db :maps))))

(defn- create-map-xml-structure
  [m]
  [:map (select-keys m [:id :name])])

(defn- create-none-building-xml-structure
  [db]
  [[:building {:id "none" :name "None"}
    (concat
      (map
        create-map-xml-structure
        (get-maps-without-building db))
      [(create-map-xml-structure {:id "none" :name "None"})])]])

(defn- create-asset-types-key-xml-strcture
  [db]
  (into
    [:assetTypes]
    (map
      (fn [asset-type]
        [:assetType (select-keys asset-type [:id :name])])
      (database/get-entities db :asset-types))))

(defn- create-place-key-xml-structure
  [db]
  [:place {:type "hierarchy"}
   (into
     [:site {:name "All"}]
     (concat
       (map
         (fn [building]
           (into
             [:building (select-keys building [:id :name])]
             (map create-map-xml-structure (get-maps-of-building-sorted-by-floors db (:id building)))))
         (database/get-entities db :buildings))
       (create-none-building-xml-structure db)))])

(defn- create-zone-grouping-key-xml-structure
  [db]
  (into
    (zone-groupings->xml (database/get-entities db :zone-groupings))
    [[:zoneGrouping {:id "none" :name "None"}
      [:zoneGroup {:id "none" :name "None"}]]]))

(defn create-report-xml-structure
  "Creates a report XML structure suitable for prxml.
  db - database
  entries - [key value] vectors"
  [db entries]
  [:report
   [:keys
    (create-place-key-xml-structure db)
    (create-asset-types-key-xml-strcture db)
    (create-zone-grouping-key-xml-structure db)]
   [:groupings [:grouping {:name "Total"} [:group {:name "Total"}]]]
   [:data (map (fn [[k v]] [:entry {:key k :value v}]) entries)]])


