(ns ekahau.vision.site-service
  (:require
   [ekahau.vision.database :as database])
  (:use
   [ekahau.vision.predicate :only
    [get-asset-predicate-by-spec-id]]
   [ekahau.vision.location-history-report.asset-states :only
    [create-states-from-asset-for-site-view-fn]]
   [ekahau.vision.location-history-report.position-states :only
    [create-states-from-position-for-site-view-fn]]))

;; ## Getting asset position

(defn index-asset-position-observations
  "Returns a map with asset ID keys and latest asset position keys."
  [db]
  (reduce
   (fn [result {id :id {position :position} :position-observation}]
     (assoc result id position))
   {} (database/get-entities db :asset-position-observations)))

(defn create-get-asset-position-fn
  "Returns a function (fn [asset] ...) that returns the latest position of the
  given asset."
  [db]
  (let [positions-by-asset-id (index-asset-position-observations db)]
    (fn [{id :id}]
      (get positions-by-asset-id id))))

;; ## States of an asset.

(defn get-site-view-asset-states
  "Returns a set of states where the given asset is in."
  [asset-position-to-states engine-asset-id-to-states asset]
  (let [position-states (asset-position-to-states asset)
        asset-states (engine-asset-id-to-states (:id asset))]
    (set
     (for [position-state position-states
           asset-state asset-states]
       [position-state asset-state]))))

(defn create-get-site-view-asset-states-fn
  "Returns a function (fn [asset] ...) that returns a set of states
  where the give asset is in."
  [db columns]
  (let [{asset-type-ids :asset-types
         asset-spec-ids :asset-specs} columns]
    (partial
     get-site-view-asset-states
     ;; Asset states by position:
     (comp (create-states-from-position-for-site-view-fn db)
           (create-get-asset-position-fn db))
     ;; Asset states by asset identity (type and properties):
     (create-states-from-asset-for-site-view-fn
      db asset-type-ids asset-spec-ids))))

(defn index-assets-by-state-pairs
  "Indexes assets by key pairs where the first key indicates an asset
  position and the second key indicates a state based on asset type or
  properties. For example the value for a key [building-a
  asset-type-b] would be a set of assets of asset-type-b with current
  position in building-a."
  [db assets columns]
  (let [get-states (create-get-site-view-asset-states-fn db columns)]
    (reduce
     (fn [result asset]
       (reduce
        (fn [result state]
          (update-in result [state] (fnil conj #{}) asset))
        result (get-states asset)))
     {} assets)))

(defn index-db-assets-by-state-pairs
  "Indexes assets of db by key pairs where the first key indicates an
  asset position and the second key indicates a state based on asset
  type or properties. For example the value for a key [building-a
  asset-type-b] would be a set of assets of asset-type-b with current
  position in building-a."
  [db columns]
  (index-assets-by-state-pairs
   db (database/get-entities db :assets) columns))

;; ## Determining cell values

;; ### Functions that decide if a row should or should not have values

(defn zone-grouping-key?
  "A predicate that returns true if the row-key is a key of a zone
  grouping."
  [row-key]
  (and (contains? row-key :zone-grouping)
       (not (contains? row-key :zone-group))))

(defn value-row?
  "A predicate that returns true it he row with the given key should
  have values."
  [row-key]
  (not
   (or (zone-grouping-key? row-key)
       (nil? row-key))))

;; ### Calculating the actual values

(defn count-assets
  "Returns a vector with count of assets that the given asset-pred
  matches to and the total count of assets. If asset-pred is nil the
  result vector has only one element (total count.)"
  [assets asset-pred]
  (if asset-pred
    [(count (filter asset-pred assets)) (count assets)]
    [(count assets)]))

(defn get-cell-value
  "Returns the value of a cell indicated by the key-pair. The result
  is vector with count of assets that the given asset-pred matches to
  and the total count of assets. If asset-pred is nil the result
  vector has only one element (total count.)"
  [assets-per-key-pair asset-pred [row column :as key-pair]]
  (when (value-row? row)
    (count-assets
     (get assets-per-key-pair key-pair)
     asset-pred)))

(defn create-get-asset-count-cell-value-fn
  "Returns a function that given a key pair returns the value of a
  given cell."
  [db columns asset-spec-id]
  (partial get-cell-value
           (index-db-assets-by-state-pairs db columns)
           (get-asset-predicate-by-spec-id db asset-spec-id)))

(defn create-get-asset-count-cell-value-of-specific-assets
  "Returns a function that given a key pair returns the value of a
  given cell."
  [db columns assets asset-spec-id]
  (partial get-cell-value
           (index-assets-by-state-pairs
            db
            assets
            columns)
           (get-asset-predicate-by-spec-id db asset-spec-id)))
