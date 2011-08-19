(ns ekahau.vision.model.building
  (:require
   [ekahau.vision.model
    [positions :as positions]]
   [ekahau.vision.database :as database]))

(defn get-floors
  [db]
  (database/get-entities db :floors))

(defn floor-with-building-id?
  [building-id]
  (fn [floor]
   (= building-id (:building-id floor))))

(defn get-building-map-ids
  [db building-id]
  (->> (get-floors db)
    (filter (floor-with-building-id? building-id))
    (map :map-id)))

(defn get-sorted-floors-of-building
  [db building-id]
  (->> (database/get-entities db :floors)
       (filter (floor-with-building-id? building-id))
       (sort-by :order-num)))

(defn get-all-floors-map-ids
  [db]
  (->> (database/get-entities db :floors)
       (map :map-id)
       (set)))

(defn map-not-in-building?
  [db]
  (let [maps-in-floors (get-all-floors-map-ids db)]
    (comp not maps-in-floors :id)))

(defn get-building-map-id-set
  [db building-id]
  (set (get-building-map-ids db building-id)))

(defn map-in-building?
  [db building-id]
  (comp boolean (get-building-map-id-set db building-id)))

(defn get-floor-of-map
  [db map-id]
  (first
   (filter
    #(= map-id (:map-id %))
    (database/get-entities db :floors))))

(defn get-asset-building
  [db asset-id]
  (->> (positions/get-map-of-asset db asset-id)
       (get-floor-of-map db)
       :building-id))

(defn asset-in-building?
  [db building-id asset]
  (= building-id (get-asset-building db (:id asset))))

(defn get-maps-not-in-any-building
  [db]
  (->> (database/get-entities db :maps)
       (filter (map-not-in-building? db))))