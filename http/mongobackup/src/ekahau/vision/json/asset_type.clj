(ns ekahau.vision.json.asset-type
  (:require
   [ekahau.vision.database :as database])
  (:use
   [ekahau.vision.model.asset :only [create-ancestor-asset-types-map
                                     get-asset-type-card]]
   [ekahau.vision.json.helpers :only [prepare-for-json]]))

(defn- fix-property-types
  [asset-type]
  (assoc asset-type :property-groups
         (map
          (fn [property-group]
            (assoc property-group :properties
                   (map
                    (fn [property]
                      (update-in property [:type] database/db-type-to-name))
                    (:properties property-group))))
          (:property-groups asset-type))))

(defn ensure-card
  [db asset-type]
  (if-not (:card-desc asset-type)
    (assoc asset-type :card-desc (get-asset-type-card db (:id asset-type)))
    asset-type))

(defn asset-type-to-json
  [ancestor-asset-types ensure-card asset-type]
  (-> asset-type
      (ensure-card)
      (fix-property-types)
      (assoc :ancestor-asset-types (get ancestor-asset-types (:id asset-type)))
      (prepare-for-json
       [:id :parent-type-id :name :icon
        :property-groups :ancestor-asset-types
        :card-desc])))

(defn create-asset-type-to-json-fn
  [db]
  (partial asset-type-to-json
           (create-ancestor-asset-types-map
            (database/get-entities db :asset-types))
           (partial ensure-card db)))
