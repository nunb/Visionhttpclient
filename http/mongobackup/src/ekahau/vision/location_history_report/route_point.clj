(ns ekahau.vision.location-history-report.route-point
  (:require ekahau.engine.stream-connection)
  (:require ekahau.engine.cmd)
  (:require [ekahau.vision.database :as database])
  (:use ekahau.vision.location-history-report.asset-states)
  (:use ekahau.vision.location-history-report.position-states)
  (:use
   [clojure.contrib.trace :only [trace]]
   [ekahau.string :only
    [parse-id parse-int parse-long]]))

(defn- position-from-emsg
  [{:keys [mapid x y zoneid modelid]}]
  (struct-map database/position
    :map-id   (parse-id mapid)
    :point    [(parse-int x) (parse-int y)]
    :zone-id  (parse-id zoneid)
    :model-id (parse-id modelid)))

(defn- parse-position-observation
  [{:keys [timestamp] :as emsg}]
  (struct-map database/position-observation
    :position  (position-from-emsg emsg)
    :timestamp (parse-long timestamp)))

(defn- route-point-from-emsg
  [{properties :properties}]
  (struct-map database/route-point
    :engine-asset-id (parse-id (:assetid properties))
    :pos-obs         (parse-position-observation properties)
    :timetoscan      (parse-long (:timetoscan properties))))

(defn- route-points-from-emsgs
  [emsgs]
  (map route-point-from-emsg emsgs))

;;

(def *report-progress* (fn [timepoint]))

(defn- with-inspector
  [coll f]
  (map
    (fn [route-point]
      (f route-point)
      route-point)
    coll))

(defn- request-route-point-seq
  [engine-asset-ids [since until]]
  (->
   (ekahau.engine.stream-connection/*request-emsg-seq*
    "route/routehistorybrowse"
    {:assetid engine-asset-ids
     :since since
     :until until
     :sort "ascending"
     :show "ZONE"})
    (route-points-from-emsgs)
    (with-inspector (fn [route-point]
                      (*report-progress*
                       (-> route-point :pos-obs :timestamp))))))

;; Route history for engine-assets ;;

(defn get-engine-assets-route-points-snapshot
  [engine-asset-ids timepoint]
  (->
   (ekahau.engine.cmd/routehistorysnapshot-by-assets engine-asset-ids timepoint)
   (route-points-from-emsgs)
   (->> (map #(assoc-in % [:pos-obs :timestamp] timepoint)))))

(defn get-engine-assets-route-points-within-interval
  [engine-asset-ids [since until :as time-interval]]
  (concat
    (get-engine-assets-route-points-snapshot engine-asset-ids since)
    (request-route-point-seq engine-asset-ids time-interval)))

;; Route history for a single engine-asset ;;

(defn get-engine-asset-route-points-since
  [engine-asset-id timepoint route-point-count]
  (-> (ekahau.engine.cmd/routehistorybrowse-by-asset-since engine-asset-id timepoint route-point-count)
    (route-points-from-emsgs)))

(defn get-engine-asset-route-points-until
  [engine-asset-id timepoint route-point-count]
  (-> (ekahau.engine.cmd/routehistorybrowse-by-asset-until engine-asset-id timepoint route-point-count)
    (route-points-from-emsgs)))

(defn get-engine-asset-route-points-within
  [engine-asset-id [since until :as time-interval]]
  (-> (ekahau.engine.cmd/routehistorybrowse-by-asset-within engine-asset-id time-interval)
    (route-points-from-emsgs)))

(defn get-latest-engine-asset-route-points
  [engine-asset-id route-point-count]
  (-> (ekahau.engine.cmd/routehistorybrowse-by-asset-latest-points engine-asset-id route-point-count)
    (route-points-from-emsgs)))
