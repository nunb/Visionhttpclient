(ns ekahau.vision.location-history-report.position-states
  (:require [ekahau.vision.database :as database])
  (:use [clojure.contrib.trace :only [trace]]))

(defn add-maps-from-maps
  "Adds map states to the result map."
  [result maps]
  (reduce
   (fn [result m]
     (update-in result [(:id m) ]
                (fnil conj #{}) {:map (:id m)}))
   result maps))

(defn add-buildings-from-floors
  "Adds building states to the result map."
  [result floors]
  (reduce
   (fn [result floor]
     (update-in result
                [(:map-id floor)]
                (fnil conj #{}) {:building (:building-id floor)}))
   result floors))

(defn add-building-all
  [result maps]
  (reduce
   (fn [result a-map]
     (update-in result
                [(:id a-map)]
                (fnil conj #{}) {:buildings :all}))
   result (cons nil maps)))

(defn add-building-none-from-maps-and-floors
  "Adds none building for each map without a floor."
  [result maps floors]
  (let [map-has-floor (set (map :map-id floors))]
   (reduce
    (fn [result a-map]
      (if-not (map-has-floor (:id a-map))
        (update-in result
                   [(:id a-map)]
                   (fnil conj #{}) {:building :none})
        result))
    result maps)))

(defn create-map-to-states-mapping
  "Returns a hash-map that has map ID keys. Values are sets of states. Each set
  always contains at least the map itself. Additionally there can be the
  building map belongs to."
  [db]
  (let [maps (database/get-entities db :maps)
        floors (database/get-entities db :floors)]
   (-> {}
       (add-maps-from-maps maps)
       (add-buildings-from-floors floors)
       (add-building-none-from-maps-and-floors maps floors))))

(defn create-map-to-states-mapping-for-site-view
  "Returns a hash-map that has map ID keys. Values are sets of states. Each set
  always contains at least the map itself. Additionally there can be the
  building map belongs to."
  [db]
  (let [maps (database/get-entities db :maps)
        floors (database/get-entities db :floors)]
    (-> {}
        (add-maps-from-maps maps)
        (add-buildings-from-floors floors)
        (add-building-none-from-maps-and-floors maps floors)
        (add-building-all maps))))

(defn add-zones-from-zones
  "Adds zone states to the result map."
  [result zones]
  (reduce
   (fn [result zone]
     (update-in result
                [(:id zone)]
                (fnil conj #{}) {:zone (:id zone)}))
   result zones))

(defn create-zone-group-entries
  "Creates a sequence of [zone-id zone-grouping] entries."
  [zone-groupings]
  (for [zone-grouping zone-groupings
        zone-group (:groups zone-grouping)
        zone-id (:zone-ids zone-group)]
    [zone-id {:zone-grouping (:id zone-grouping)
              :zone-group (:id zone-group)}]))

(defn add-zone-groups-from-zone-groupings
  "Adds zone group states to the result map."
  [result zone-groupings]
  (reduce
   (fn [result [zone-id state]]
     (update-in result [zone-id]
                (fnil conj #{}) state))
   result
   (create-zone-group-entries zone-groupings)))

(defn create-zone-to-states-mapping
  "Returns a hash-map that has zone ID keys. Values are sets of states. Each set
  always contains at least the zone itself. Additionally there can be zero or
  more zone groups the specific zone belongs to."
  [db]
  (-> {}
      (add-zones-from-zones
       (database/get-entities db :zones))
      (add-zone-groups-from-zone-groupings
       (database/get-entities db :zone-groupings))))

(defn create-states-from-position-fn
  "Returns a function that given a position returns a set of states that
  position is associated with. The function uses the state of database when
  this function is called."
  [db]
  (dosync
   (let [map-to-states (create-map-to-states-mapping db)
         zone-to-states (create-zone-to-states-mapping db)]
     (fn [position]
       (clojure.set/union
        (map-to-states (:map-id position))
        (zone-to-states (:zone-id position)))))))

(defn create-states-from-position-for-site-view-fn
  "Returns a function that given a position returns a set of states that
  position is associated with. The function uses the state of database when
  this function is called."
  [db]
  (dosync
   (let [map-to-states (create-map-to-states-mapping-for-site-view db)
         zone-to-states (create-zone-to-states-mapping db)]
     (fn [position]
       (if position
        (clojure.set/union
         (map-to-states (:map-id position))
         (zone-to-states (:zone-id position)))
        #{{:buildings :all} {:building :none} {:map :none} {:zone :none}})))))
