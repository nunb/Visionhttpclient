(ns ekahau.vision.json.asset
  (:require
   [ekahau.vision.database :as database])
  (:use
   [clojure.contrib.trace :only [trace]]
   [ekahau.vision.json.helpers
    :only [prepare-for-json]]
   [ekahau.vision.routes.helpers
    :only [str-id-path]]
   [ekahau.vision.xml.asset
    :only [format-property]]
   [ekahau.vision.json.asset-type
    :only [create-asset-type-to-json-fn]]
   [ekahau.vision.model.asset
    :only [create-get-property-type-by-id-fn]]))

(defn- format-json-asset-property
  [get-property-type-by-id [k v]]
  (let [type (get-property-type-by-id k)]
    [(str-id-path k)
     (format-property v (get-property-type-by-id k))]))

(defn- asset-properties-to-json
  [get-property-type-by-id asset]
  (update-in asset [:properties]
    (fn [properties]
      (into {}
        (map
          (partial format-json-asset-property get-property-type-by-id)
          properties)))))

(defn asset-to-json
  [get-property-type-by-id asset]
  (-> (asset-properties-to-json get-property-type-by-id asset)
      (prepare-for-json [:id :asset-type-id :properties
                         :tag-information :position-information
                         :to-do-list-information])))

(defn assets-and-types-of-report-for-json
  [db assets-report]
  {:assetTypes
   (map
    (create-asset-type-to-json-fn db)
    (:asset-types assets-report))

   :assets
   (map
    (partial asset-to-json (create-get-property-type-by-id-fn
                            (:asset-types assets-report)))
    (:assets assets-report))


   :zoneGroupings
   (map
    #(select-keys % [:id :name])
    (database/get-entities db :zone-groupings))})
