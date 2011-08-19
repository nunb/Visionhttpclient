(ns ekahau.vision.location-service
  (:require
    [ekahau.vision.database :as database]
    [ekahau.engine.locations :as erc])
  (:use
    [ekahau.vision.model.asset :only [get-engine-asset-ids
                                      get-asset-by-engine-asset-id]]
    [ekahau.vision.location-history-report.route-point :only [get-engine-assets-route-points-snapshot]]
    [ekahau.vision.par-management :only [init-par-manager-state
                                         invoke-par-management!]]
    [clojure.contrib.logging :only [debug info warn error]])
  (:import
    [java.util Date]
    [java.util.concurrent BlockingQueue Executors TimeUnit]
    [com.ekahau.engine.sdk DeviceLocation]))

;; Updating Location ;;

(defn update-asset-position-observation!
  [db id observation]
  (if (not (nil? id))
    (database/put-entity! db :asset-position-observations
      (struct database/entity-position-observation id observation))
    db))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;; Location Management ;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Create Location Manager ;;

(defn create-location-manager
  [erc-config]
  {:loc-tracker      (erc/create-location-tracking-engine
                       erc-config)
   :par-manager      (agent nil)
   :par-historian    (agent nil)
   :location-handler (Executors/newSingleThreadExecutor)})

;; Start Managing Locations ;;

(defn- get-location-snapshot
  [db timestamp]
  (get-engine-assets-route-points-snapshot
    (get-engine-asset-ids db) timestamp))

(defn- populate-asset-position-observations!
  [db location-snapshot]
  (-> db
    (database/delete-all-entities! :asset-position-observations)
    (database/put-entities! :asset-position-observations
      (remove nil?
        (map (fn [{:keys [engine-asset-id pos-obs]}]
               (let [asset-id (:id (get-asset-by-engine-asset-id
                                     db engine-asset-id))]
                 (struct-map database/entity-position-observation
                   :id asset-id :position-observation pos-obs)))
          location-snapshot)))))

(defn start-tracking-locations!
  [{:keys [loc-tracker]}]
  (info "Starting ERC location tracking ...")
  (erc/start-tracking-locations! loc-tracker))

(defn start-handling-locations!
  [{:keys [loc-tracker par-manager location-handler]} db {:as opts}]
  (try
    (let [snapshot-ts (System/currentTimeMillis)]
      (info (str "Getting route points snapshot ..."))
      (let [snapshot (get-location-snapshot db snapshot-ts)]
        (info "Populating asset-position-observations ...")
        (populate-asset-position-observations! db snapshot))
      (send par-manager init-par-manager-state db)
      (release-pending-sends) ;not strictly necessary here
      (let [loc-handler-fns [update-asset-position-observation!
                             (partial send par-manager invoke-par-management!)]]
        (.execute location-handler
          (fn []
            (info "Starting location handler ...")
            (try
              ;; It is critical not to hold on to the head of the loc-seq
              (doseq [loc (drop-while
                            #(<= (:timestamp (:position-observation %)) snapshot-ts)
                            (erc/location-seq loc-tracker))]
                (try
                  (debug (str "Handling location: " loc))
                  (when-let [asset-id (:id (get-asset-by-engine-asset-id db
                                             (:id loc)))]
                    (doseq [f loc-handler-fns]
                      (f db asset-id (:position-observation loc))))
                  (catch Exception e
                    (error "Error in location handler" e))))
              (info "Awaiting par manager ...")
              (await par-manager)
              (catch Exception e
                (error "Error in location handler" e)))))))
    (catch Exception e
      (error "Error starting location manager" e))))

;; Stop Managing locations ;;

(defn stop-tracking-locations!
  [{:keys [loc-tracker]}]
  (info "Stopping ERC location tracking ...")
  (erc/stop-tracking-locations! loc-tracker))

(defn stop-handling-locations!
  [{:keys [location-handler]}]
  (try
    (info "Awaiting location-handler ...")
    (.shutdown location-handler)
    (while (not (.isTerminated location-handler))
      (.awaitTermination location-handler 1 TimeUnit/SECONDS))
    (info "Stopped handling locations.")
    (catch Exception e
      (error "Error stopping location manager" e))))
