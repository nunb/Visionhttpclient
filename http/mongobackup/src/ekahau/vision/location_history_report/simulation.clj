(ns ekahau.vision.location-history-report.simulation
  (:require
   ekahau.vision.location-history-report-service)
  (:require
   [ekahau.vision.database :as database])
  (:use
   [clojure.contrib.trace :only [trace]]))

(defn create-position-observation
  [time map-id zone-id]
  (struct-map database/position-observation
    :timestamp time
    :position (struct-map database/position
                :map-id map-id
                :zone-id zone-id
                :point [0 0])))

(defn create-route-points
  [{[start-time stop-time] :time-interval
    route-point-count :count
    zone-ids :zone-ids
    engine-asset-ids :engine-asset-ids
    :as specs}]
  (let [total-duration (- stop-time start-time)
        time-increment (/ total-duration route-point-count)
        times (iterate (partial + time-increment) start-time)]
    (mapcat
     (fn [time _]
       (map
        (fn [engine-asset-id]
          (struct-map database/route-point
            :engine-asset-id engine-asset-id
            :pos-obs         (struct-map database/position-observation
                               :timestamp time
                               :position (struct-map database/position
                                           :map-id "0"
                                           :zone-id (rand-nth zone-ids)))))
        engine-asset-ids))
     times
     (range route-point-count))))

(defn generate-route-points
  [spec engine-asset-ids time-interval]
  (create-route-points (merge spec
                              {:engine-asset-ids engine-asset-ids
                               :time-interval time-interval})))

(defn set-routepoint-generator!
  [spec]
  (reset! ekahau.vision.location-history-report-service/route-generator-atom
          (partial generate-route-points spec)))

(defn set-simulated-route-points
  [spec]
  (reset! ekahau.vision.location-history-report-service/short-circuited-results
          (create-route-points spec)))

(defn set-assets-on-random-zones!
  [db assets]
  (let [zones (vec (database/get-entities db :zones))
        random-zones (repeatedly (fn [] (rand-nth zones)))]
    (database/put-entities! db :asset-position-observations
                            (map
                             (fn [asset zone]
                               (struct-map database/entity-position-observation
                                 :id (:id asset)
                                 :position-observation
                                 (create-position-observation
                                  (System/currentTimeMillis)
                                  (get-in zone [:area :map-id])
                                  (:id zone))))
                             assets random-zones))))

(defn set-all-assets-on-random-zones!
  [db]
  (set-assets-on-random-zones! db (database/get-entities db :assets)))

(defn add-new-assets!
  [db [asset-type-id property-group-id property-id :as property-key] values]
  (->>
   (map
    (fn [value]
      (struct-map database/asset
        :asset-type-id asset-type-id
        :properties [[property-key value]]))
    values)
   (database/assoc-new-entity-ids! db :assets)
   (database/put-entities! db :assets)))

(defn entities-of-type-missing-value-of-property
  [db asset-type-id property-key]
  (->> (database/get-entities db :assets)
       (filter #(= asset-type-id (:asset-type-id %)))
       (filter #(nil? (ekahau.vision.model.asset/get-asset-property
                       % property-key)))))

(defn set-assets-property-value
  [db asset-ids property-key value]
  (reduce
   (fn [db id]
     (database/put-entity! db :assets
      (update-in (database/get-entity-by-id db :assets id) [:properties]
                 (fn [properties]
                   (conj
                    (remove (fn [[k v]] (= k property-key)) properties)
                    [property-key value])))))
   db
   asset-ids))

(defn assign-engine-asset-ids-to-asset-ids
  [db]
  (let [used-engine-asset-ids (->> (database/get-entities db :assets)
                                   (map :engine-asset-id)
                                   (remove nil?)
                                   (set))
        new-engine-asset-ids (remove used-engine-asset-ids
                                     (map str (iterate inc 0)))]
    (database/put-entities!
     db :assets
     (map
      (fn [asset engine-asset-id]
        (assoc asset
          :engine-asset-id engine-asset-id))
      (remove :engine-asset-id (database/get-entities db :assets))
      new-engine-asset-ids))))

(defn load-names
  []
  (->> (slurp "/usr/share/dict/propernames")
       (clojure.contrib.string/split-lines)))

(defn prepare-db-for-reports
  [db]
  (assign-engine-asset-ids-to-asset-ids db)
  (set-routepoint-generator! {:zone-ids (map
                                         :id
                                         (database/get-entities db :zones))
                              :count 100}))