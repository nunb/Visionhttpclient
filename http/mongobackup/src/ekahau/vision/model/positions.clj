(ns ekahau.vision.model.positions
  (:require
    [ekahau.vision.database :as database])
  (:use
    [ekahau.vision.model.zone-grouping :only [get-zone-grouping-info-for-zone-id]]))

(defn get-map-by-id
  [db map-id]
  (database/get-entity-by-id db :maps map-id))

(defn get-model-by-id
  [db model-id]
  (database/get-entity-by-id db :models model-id))

(defn- get-floors
  [db]
  (database/get-entities db :floors))

(defn get-floor-by-id
  [db f-id]
  (database/get-entity-by-id db :floors f-id))

(defn get-building-by-id
  [db b-id]
  (database/get-entity-by-id db :buildings b-id))

(defn get-floor-from-map-id
  [db map-id]
  (first (filter #(= map-id (:map-id %))
           (get-floors db))))

(defn get-building-from-map-id
  [db map-id]
  (get-building-by-id db
    (:building-id (get-floor-from-map-id db map-id))))

(defn get-zone-by-id
  [db zone-id]
  (database/get-entity-by-id db :zones zone-id))

(defn get-zones
  [db]
  (database/get-entities db :zones))

(defn get-zone-map-id
  [db zone]
  (:map-id (:area zone)))

(defn form-detailed-position
  [db {{map-id :id} :map, {model-id :id} :model,
       {zone-id :id} :zone, point :point}]
  (struct-map database/detailed-position
    :map           (-> (into {} (get-map-by-id db map-id))
                     (dissoc :image)
                     (dissoc :scale))
    :point         point
    :model         (get-model-by-id db model-id)
    :floor         (get-floor-from-map-id db map-id)
    :building      (get-building-from-map-id db map-id)
    :site          nil ; for now
    :zone          (-> (get-zone-by-id db zone-id)
                     (update-in [:area] #(dissoc (into {} %) :polygon)))
    :zone-grouping (get-zone-grouping-info-for-zone-id db zone-id)))

(defn form-detailed-position-from-zone-id
  [db zone-id]
  (form-detailed-position db
    {:zone {:id zone-id}
     :map {:id (get-zone-map-id db
                 (get-zone-by-id db zone-id))}}))

;; Position Observations ;;

(defn get-asset-position-observation
  [db asset-id]
  (database/get-entity-by-id db :asset-position-observations asset-id))

(defn- get-asset-position-observation-by-id-map
  [db]
  (->> (database/get-entities db :asset-position-observations)
       (map (juxt :id identity))
       (into {})))

(defn create-get-asset-position-observation-fn
  "Creates a function that takes one asset parameter and returns the asset
  position observation in the given db."
  [db]
  (let [m (get-asset-position-observation-by-id-map db)]
    (fn [asset]
      (:position-observation (get m (:id asset))))))

(defn get-asset-position-observations-on-map
  [db map-id]
  (->> (database/get-entities db :asset-position-observations)
       (filter #(= map-id
                   (get-in % [:position-observation :position :map-id])))))

;; Maps ;;

(defn create-get-asset-map-id-fn
  [db]
  (let [get-position-observation (create-get-asset-position-observation-fn db)]
    (fn [asset]
      (-> (get-position-observation asset) :position :map-id))))

(defn create-get-asset-zone-id-fn
  [db]
  (let [get-position-observation (create-get-asset-position-observation-fn db)]
    (fn [asset]
      (-> (get-position-observation asset) :position :zone-id))))

(defn get-map-of-asset
  [db asset-id]
  (get-in (get-asset-position-observation db asset-id)
          [:position-observation :position :map-id]))

(defn asset-on-map?
  [db map-id asset]
  (= map-id (get-map-of-asset db (:id asset))))

(defn asset-on-map-set?
  [db map-set asset]
  (map-set (get-map-of-asset db (:id asset))))

(defn asset-on-no-map?
  [db asset]
  (nil? (get-map-of-asset db asset)))

;; Buildings ;;

(defn create-building-id-by-map-id-map
  [db]
  (reduce
    (fn [m {:keys [building-id map-id]}]
      (assoc m map-id building-id))
    {}
    (database/get-entities db :floors)))

(defn create-get-asset-building-id-fn
  "Creates a function that takes an asset as a parameter and returns the current building-id."
  ([db]
    (create-get-asset-building-id-fn db (create-get-asset-map-id-fn db)))
  ([db get-asset-map-id]
    (let [building-id-by-map-id (create-building-id-by-map-id-map db)]
      (fn [asset]
        (get building-id-by-map-id (get-asset-map-id asset))))))

;; No Position ;;

(defn get-assets-without-position
  [db assets]
  (let [get-asset-map-id (create-get-asset-map-id-fn db)]
    (filter (comp nil? get-asset-map-id) assets)))
