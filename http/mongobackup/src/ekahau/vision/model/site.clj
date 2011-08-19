(ns ekahau.vision.model.site
  (:require
   [ekahau.vision.database :as database]))

(defprotocol Site
  (get-zones [site])
  (get-zones-of-map [site map-id])

  (get-floors-of-building [site building-id])
  
  (get-maps [site])
  (get-map-by-id [site map-id])
  (get-maps-of-building [site building-id])
  (get-maps-in-no-building [site])
  
  (get-buildings [site]))

(deftype DatabaseSite [db]
  Site
  (get-zones
   [site]
   (database/get-entities db :zones))
  
  (get-zones-of-map
   [site map-id]
   (database/search-entities db :zones [["=" [:area :map-id] map-id]]))

  (get-maps
   [site]
   (database/get-entities db :maps))

  (get-map-by-id
   [site map-id]
   (database/get-entity-by-id db :maps map-id))
  
  (get-floors-of-building
   [site building-id]
   (database/search-entities db :floors
                             [["=" [:building-id] building-id]]
                             {:order-by [[[:order-num] 1]]}))
  
  (get-maps-of-building
   [site building-id]
   (let [floors (get-floors-of-building site building-id)
         map-ids (set (map :map-id floors))
         maps (database/search-entities db :maps
                                        [["IN" [:id] map-ids]])
         indexed-by-id (zipmap (map :id maps) maps)]
     (map #(indexed-by-id (:map-id %)) floors)))

  (get-maps-in-no-building
   [site]
   (let [map-ids (set (map :map-id (database/get-entities db :floors)))]
    (database/search-entities db :maps
                              [["NOT-IN" [:id] map-ids]])))
  
  (get-buildings
   [site]
   (database/get-entities db :buildings)))

(defn new-database-site
  [db]
  (DatabaseSite. db))
