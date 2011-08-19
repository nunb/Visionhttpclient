(ns ekahau.vision.grouping
  (:require
    [ekahau.vision.database :as database]
    [ekahau.vision.model
     [positions :as positions]
     [building :as building]])
  (:use
    [ekahau.vision.model.asset :only (get-all-asset-types)]
    [ekahau.util :only [assoc-multi]]))

(defn- assoc-val-to-keys
  [m val ks]
  (reduce (fn [m k] (assoc-multi m k val #{})) m ks))

(defn- multi-group-by
  [coll get-keys]
  (reduce
    (fn [m x]
      (assoc-val-to-keys m x (get-keys x)))
    {}
    coll))

(defn- get-all-asset-type-ids
  [db asset]
  (->> (get-all-asset-types db (:asset-type-id asset))
    (map :id)
    (set)))

(defn create-get-all-asset-type-ids-fn
  "Returns a function that returns a set of asset type IDs of given asset that
  the asset is type of."
  [db]
  (fn [asset]
    (set (get-all-asset-type-ids db asset))))

(defn asset-in-building?
  [db building-id]
  (let [get-asset-map-id (positions/create-get-asset-map-id-fn db)
        map-in-building? (building/map-in-building? db building-id)]
    (comp map-in-building? get-asset-map-id)))

;; New Wave ;;

;; Generic Grouping Methods ;;

(defn- group-entities-by-grouping-fn
  [entities groups get-mapping-value-from-group group-entities-by-mapping-value]
  (let [entities-by-mapping-value (group-entities-by-mapping-value entities)]
    (map
      (fn [group]
        {:attribute group
         :values (get entities-by-mapping-value
                      (get-mapping-value-from-group group))})
      groups)))

(defn- group-entities
  [entities groups get-mapping-value-from-entity get-mapping-value-from-group]
  (group-entities-by-grouping-fn
   entities
   groups
   get-mapping-value-from-group
   (partial group-by get-mapping-value-from-entity)))

(defn- multi-group-entities
  [entities groups get-mapping-values-from-entity get-mapping-value-from-group]
  (group-entities-by-grouping-fn
   entities
   groups
   get-mapping-value-from-group
   #(multi-group-by % get-mapping-values-from-entity)))

;; Buildings ;;

(defn get-building
  [db building assets]
  (when building
    (first
      (group-entities
        assets
        [building]
        (positions/create-get-asset-building-id-fn db)
        :id))))

(defn get-buildings
  [db buildings assets]
  (group-entities
    assets
    buildings
    (positions/create-get-asset-building-id-fn db)
    :id))

;; Floors ;;

(defn- floor-to-map-entry
  [db floor]
  {:floor floor
   :map   (database/get-entity-by-id db :maps (:map-id floor))})

(defn get-sorted-building-floor-map-entries
  [db building-id]
  (->> (building/get-sorted-floors-of-building db building-id)
       (map (partial floor-to-map-entry db))))

(defn get-building-floors
  [db building assets]
  (group-entities
    assets
    (get-sorted-building-floor-map-entries db (:id building))
    (positions/create-get-asset-map-id-fn db)
    (comp :id :map)))

;; Maps ;;

(defn get-maps-not-in-any-building
  [db maps assets]
  (group-entities
    assets
    (filter (building/map-not-in-building? db) maps)
    (positions/create-get-asset-map-id-fn db)
    :id))

;; Asset Types ;;

(defn get-asset-types
  [db assets asset-types]
  (multi-group-entities
    assets
    asset-types
    (create-get-all-asset-type-ids-fn db)
    :id))

;; Place grouping ;;

(defn- index-by
  ([get-key entities]
    (->> entities
      (map #(vector (get-key %) %))
      (into {}))))

(defn- get-position-group-by-map-id
  [floors-by-map-id id]
  (if-let [floor (get floors-by-map-id id)]
    [{:type :site}
     {:type :building :id (:building-id floor)}
     {:type :map :id id}]
    [{:type :site}
     {:type :map :id id}]))

(defn- create-map-id-to-position-group
  [db]
  (let [floors-by-map-id (index-by :map-id (database/get-entities db :floors))]
    (->> (database/get-entities db :maps)
      (map
        (fn [{id :id}]
          [id (get-position-group-by-map-id floors-by-map-id id)]))
      (into {}))))

(defn create-route-point-to-site-grouping
  [db]
  (let [map-id-to-position (create-map-id-to-position-group db)]
    (fn [route-point]
      (get
       map-id-to-position
       (get-in route-point [:pos-obs :position :map-id])))))
