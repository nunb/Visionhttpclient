(ns ekahau.engine.locations
  (:require
    [ekahau.vision.database :as database])
  (:use
    [ekahau.queue :only [seq-from-blocking-queue]]
    [clojure.contrib.logging :only [debug info warn error]])
  (:import
    [java.util.concurrent BlockingQueue LinkedBlockingQueue]
    [com.ekahau.engine.sdk PositioningEngine IDeviceLocationListener DeviceLocation]))

(defonce *locs-count* (atom 0))
(defonce *loc-stats-logger* (agent nil))
(def *loc-stats-update-freq* 60000) ; 1 min

(defn- log-loc-stats
  [{:keys [prev-locs last-lps overall-lps]}]
  (info (str "Total-locs:[" prev-locs  "]"
          " Locations per second:"
          " Last interval: [" last-lps "]"
          " Overall: [" overall-lps "]")))

(defn- start-logging-loc-stats!
  []
  (future ; preventing from sends in an agent
    (reset! *locs-count* 0)
    (send *loc-stats-logger*
      (fn [_]
        (let [curr (System/currentTimeMillis)]
          {:running     true
           :prev-locs   0
           :start-time  curr
           :prev-time   curr
           :last-lps    0 ; lps=locs per second
           :overall-lps 0})))
    (send *loc-stats-logger*
      (fn this [{:keys [running prev-locs prev-time start-time] :as state}]
        (if running
          (try
            (send *agent* this)
            (Thread/sleep *loc-stats-update-freq*)
            (let [timestamp (System/currentTimeMillis)
                  total-locs @*locs-count*
                  state (assoc state
                          :prev-locs   total-locs
                          :prev-time   timestamp
                          :last-lps    (* 1000 (float (/ (- total-locs prev-locs)
                                                        (- timestamp prev-time))))
                          :overall-lps (* 1000 (float (/ total-locs
                                                        (- timestamp start-time)))))]
              (log-loc-stats state)
              state)
            (catch Exception e
              (error "Error logging location" e)
              state))
          state)))))

(defn- stop-logging-loc-stats!
  []
  (future
    (send *loc-stats-logger*
      (fn [state]
        (info (str "Total locations: [" @*locs-count* "]"))
        (assoc state :running false)))))

(defn- parse-location
  [^DeviceLocation location]
  (when (.getMapId location)
    (struct-map database/entity-position-observation
      :id (str (.getAssetId location))
      :position-observation (struct-map database/position-observation
                              :position (struct-map database/position
                                          :map-id   (str (.getMapId location))
                                          :zone-id  (str (.getZoneId location))
                                          :model-id (str (.getModelId location))
                                          :point    [(.getX location), (.getY location)])
                              :timestamp (.getTimeStamp location)))))

(defn- create-queueing-location-listener
  [^BlockingQueue queue]
  (proxy [IDeviceLocationListener] []
    (newDeviceLocation [^DeviceLocation location]
      (swap! *locs-count* inc)
      (when-let [location (parse-location location)]
        (.put queue location)))
    (connectionClosed  [^Exception reason]
      (when-not (= "client called close" (.getMessage reason))
        (error "Location's IDeviceLocationListener.connectionClosed called" reason))
      (Thread/interrupted) ;; hack needed coz ERC interrupts the thread, this clears the flag
      (.put queue ::loc-terminator))))

(defn create-location-tracking-engine
  [{:keys [host port user password]}]
  (let [queue (LinkedBlockingQueue.)]
    {:loc-queue  queue
     :pos-engine (doto (PositioningEngine. host port user password)
                   (.addLocationListener
                     (create-queueing-location-listener queue)))}))

(defn start-tracking-locations!
  [{:keys [loc-queue pos-engine]}]
  (start-logging-loc-stats!)
  (.startTracking ^PositioningEngine pos-engine))

(defn stop-tracking-locations!
  [{:keys [loc-queue pos-engine]}]
  (.stopTracking ^PositioningEngine pos-engine)
  (stop-logging-loc-stats!))

(defn location-seq
  [{:keys [loc-queue pos-engine]}]
  (seq-from-blocking-queue loc-queue #(= ::loc-terminator %)))
