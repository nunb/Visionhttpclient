(ns ekahau.tools.edu-data
  (:require 
    clojure.xml
    [ekahau.vision.database :as database])
  (:use
    [clojure.contrib.pprint :only [pprint]])
  (:import
    [java.io File]))

(defn parse-edu-assets 
  [^String filename]
  (let [data (clojure.xml/parse (File. filename))]
    data))

(defn has-tag? 
  [elem tag]
  (= tag (:tag elem)))


(defn find-elements-with-tag
  [elem tag]
  (filter #(has-tag? % tag) (:content elem)))

(defn find-first-element-with-tag
  [elem tag]
  (first (find-elements-with-tag elem tag)))

(defn get-text 
  [elem]
  (first (:content elem)))

(defn parse-asset
  [elem]
  (reduce #(assoc %1 (:tag %2) (get-text %2)) {} (:content elem)))

(defn parse-type
  [elem]
  (let [type-name (get-text (find-first-element-with-tag elem :typename))
        attributes (map get-text (find-elements-with-tag elem :attribute))]
    {
     :name type-name
     :attributes attributes
     :types (map parse-type (find-elements-with-tag elem :type))
     :assets (map parse-asset (find-elements-with-tag elem :asset))
     }))

(declare assign-id-to-asset-types)

(defn assign-id-to-asset-type
  "Returns a pair of asset type with assigned id and the next available id."
  [asset-type next-available-id]
  (let [id next-available-id
        [asset-types next-available-id] (assign-id-to-asset-types (:types asset-type) (inc next-available-id))]
    [(assoc asset-type :id id :types asset-types) next-available-id]))

(defn- assign-id-to-asset-types*
  [asset-types next-available-id]
  (reduce
    (fn [state asset-type]
      (let [[updated-asset-type new-next-available-id] (assign-id-to-asset-type asset-type (:id state))]
        (-> state
          (update-in [:results] conj updated-asset-type)
          (assoc :id new-next-available-id))))
    {:id next-available-id, :results []}
    asset-types))

(defn assign-id-to-asset-types
  [asset-types next-available-id]
  (let [{:keys [id results]} (assign-id-to-asset-types* asset-types next-available-id)]
    [results id]))


(defn assign-parent-type-ids
  [asset-type parent-id]
  (-> asset-type
    (assoc :parent-type-id parent-id)
    (update-in [:types]
      (fn [child-types]
        (map #(assign-parent-type-ids % (:id asset-type)) child-types)))))


(defn assign-type-ids-to-assets
  [asset-type]
  (-> asset-type
    (update-in [:assets] (fn [assets] (map (fn [asset] {:asset-type-id (:id asset-type) :attributes asset}) assets)))
    (update-in [:types] (fn [types] (map (fn [child-type] (assign-type-ids-to-assets child-type)) types)))))


(defn attributes-to-property-group
  [asset-type]
  (-> asset-type
    (assoc :property-groups
      [
       (struct database/property-group
         0
         (:name asset-type)
         (vec (map (fn [id label] (struct database/property id label "database/text")) (iterate inc 0) (:attributes asset-type))))
       ])
    (update-in [:types] (fn [types] (map (fn [child-type] (attributes-to-property-group child-type)) types)))))


(defn attribute-labels-to-property-key-map
  [asset-type]
  (apply merge
    (zipmap (map keyword (:attributes asset-type)) (map #(vector (:id asset-type) 0 %) (iterate inc 0)))
    (map attribute-labels-to-property-key-map (:types asset-type))))


(defn convert-attributes-to-properties
  [attributes m]
  (map (fn [[k v]] [(get m k) v]) attributes))

(defn convert-to-database-assets
  [assets m]
  (map
    (fn [asset]
      (struct database/asset nil (:asset-type-id asset) (convert-attributes-to-properties (:attributes asset) m)))
    assets))

(defn get-assets
  ([asset-type]
    (map #(assoc %1 :id %2) (get-assets asset-type (attribute-labels-to-property-key-map asset-type)) (iterate inc 0)))
  ([asset-type m]
    (concat (convert-to-database-assets (:assets asset-type) m) (mapcat #(get-assets % m) (:types asset-type)))))

(defn get-asset-types
  [asset-type]
  (cons
    (select-keys asset-type [:id :parent-type-id :name :property-groups])
    (mapcat get-asset-types (:types asset-type))))

(defn parse-assets-and-types
  [xml]
  (-> xml
    (parse-type)
    (assign-id-to-asset-type 0)
    first
    (assign-parent-type-ids nil)
    (assign-type-ids-to-assets)
    (attributes-to-property-group)))

(defn load-to-database
  [db f]
  (let [xml (clojure.xml/parse f)
        phase (parse-assets-and-types xml)]
    (-> db
      (database/put-entities! :asset-types (get-asset-types phase))
      (database/put-entities! :assets (get-assets phase)))))
