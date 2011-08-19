(ns ekahau.vision.export-service
  (:require
    [ekahau.vision.database :as database])
  (:use
    [ekahau.util                       :only [write-csv]]
    [ekahau.vision.event-service       :only [get-events         get-event-by-id]]
    [ekahau.vision.event-rule-service  :only [get-event-rules    get-event-rule-by-id]]
    [ekahau.vision.model.asset         :only [get-assets         get-asset-by-id
                                              get-asset-types    get-asset-type-by-id]]
    [ekahau.vision.model.positions     :only [get-zones          get-zone-by-id]]
    [ekahau.vision.model.zone-grouping :only [get-zone-groupings get-zone-grouping-by-id]]))

(defn- mk-key-fn
  [struct-type]
  #(keys (struct-map struct-type)))

(def entities-export-map
  {:assets         [get-assets              get-asset-by-id         (mk-key-fn database/asset)]
   :asset-types    [get-asset-types         get-asset-type-by-id    (mk-key-fn database/asset-type)]
   :events         [get-events              get-event-by-id         (mk-key-fn database/event)]
   :event-rules    [get-event-rules         get-event-rule-by-id    (mk-key-fn database/event-rule)]
   :zones          [get-zones               get-zone-by-id          (mk-key-fn database/zone)]
   :zone-groupings [get-zone-groupings      get-zone-grouping-by-id (mk-key-fn database/zone-grouping)]})

(defn- get-entities
  ([db entity-kw]
    ((first (get entities-export-map entity-kw)) db))
  ([db entity-kw ids]
    (map (partial (second (get entities-export-map entity-kw)) db) ids)))

(defn- get-entity-fields
  [db entity-kw]
  ((nth (get entities-export-map entity-kw) 2)))

(defn- to-csv
  [db entity-kw entities]
  (write-csv entities
    (get-entity-fields db entity-kw)))

(defn get-entities-as-csv
  ([db entity-kw]
    (to-csv db entity-kw
      (get-entities db entity-kw)))
  ([db entity-kw ids]
    (to-csv db entity-kw
      (get-entities db entity-kw ids))))
