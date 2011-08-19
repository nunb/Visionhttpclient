(ns ekahau.vision.par-history-service
  (:require
    [ekahau.vision.database :as database])
  (:use
    [ekahau.string :only [parse-long]]
    [ekahau.vision.par-snapshot :only [get-curr-zoneid
                                       get-assets-count
                                       update-par-snapshot]]
    [ekahau.vision.model.asset :only [get-assets
                                      get-asset-by-engine-asset-id]]
    [ekahau.engine.cmd :only [routehistorybrowse
                              routehistorysnapshot-by-assets]]
    [ekahau.engine.stream-connection :only [*request-emsg-seq*]]
    [ekahau.vision.engine-service :only [get-ERC-route-history-info!]]
    [clojure.contrib.logging :only [debug info error]]))

(defn- hb-erc-params
  [engine-asset-ids [since until]]
  {:assetid engine-asset-ids
   :since   since
   :until   until
   ;:format  "compact"
   :sort    "ascending"
   :show    "ZONE"})

(def location-history-non-streaming
  (comp routehistorybrowse hb-erc-params))

(def location-history-streaming
  (comp #(*request-emsg-seq* "route/routehistorybrowse" %) hb-erc-params))

(def *location-history-fn* (var location-history-non-streaming))

(def *max-erc-query-timeframe* (* 1000 60 60)) ; one hour
(def *par-data-granularity*    (* 1000 60 1))  ; one minute

(defn- persist-par-history!
  [db history]
  (database/put-entities! db :par-history history))

(defn- compactify
  [par-history]
  (map (fn [[k v]]
         (merge k {:count (:count (last v))}))
    (group-by (fn [p] (-> p
                        (dissoc :count)
                        (update-in [:timestamp]
                          #(* %2 (long (/ %1 %2))) *par-data-granularity*)))
      par-history)))

(defn- mk-phr
  [par-snapshot zoneid atid timestamp]
  {:zone-id       zoneid
   :asset-type-id atid
   :timestamp     timestamp
   :count         (get-assets-count par-snapshot zoneid atid)})

; XXX make this zone-id specific
(defn- form-and-persist-par-history!
  [db asset-type-ids start end]
  (let [assets (database/search-entities db :assets
                 [["IN" [:asset-type-id] asset-type-ids]])
        e-at-m (into {} (map #(vector (:engine-asset-id %)
                                (:asset-type-id %))
                          assets))
        eids (remove nil? (keys e-at-m))
        upd-par-snapshot #(update-par-snapshot %1
                            (assoc %2 :assettypeid (get e-at-m (:assetid %2))))]
    (loop [ts-ranges (partition 2 1
                       (concat (range start end *max-erc-query-timeframe*) [end]))
           par-snapshot (reduce upd-par-snapshot {}
                          (map :properties
                            (routehistorysnapshot-by-assets eids start)))
           total-count 0]
      (if-let [[start end] (first ts-ranges)]
        (do
          (debug (str "Populating par-history for: " [start end asset-type-ids]))
          (let [[updated-par-snapshot par-history]
                (reduce
                  (fn [[par-snapshot par-history] {:keys [assetid zoneid timestamp] :as loc}]
                    (let [timestamp (parse-long timestamp)
                          atid (get e-at-m assetid)
                          oldzid (get-curr-zoneid par-snapshot assetid)
                          updated-par-snapshot (upd-par-snapshot par-snapshot loc)]
                      [updated-par-snapshot
                       (-> (if oldzid
                             (conj par-history (mk-phr updated-par-snapshot
                                                 oldzid atid timestamp))
                             par-history)
                         (conj (mk-phr updated-par-snapshot
                                 zoneid atid timestamp)))]))
                  [par-snapshot []]
                  (map :properties
                    (*location-history-fn* eids [start end])))]
            (do
              (let [par-history (compactify par-history)]
                (persist-par-history! db par-history)
                (recur (rest ts-ranges) updated-par-snapshot
                  (+ total-count (count par-history)))))))
        total-count))))

; XXX make this zone-id specific
(defn- update-fetch-ts!
  [db asset-type-ids start end time-taken rows-count]
  (database/put-entity! db :par-history-fetch-timestamps
    {:time-taken     time-taken
     :rows-count     rows-count
     :asset-type-ids asset-type-ids
     :end            end
     :start          start}))

(defn- get-intervals
  [fts start end] 
  (loop [fts fts start start end end invts '()]
    (if-let [{s :start e :end} (first fts)]
      (cond
        (<= s start end e) invts
        (<= start end s e) (recur (rest fts) start end invts)
        (<= start s end e) (recur (rest fts) start s invts)
        (<= s start e end) (cons [e end] invts)
        (<= s e start end) (cons [start end] invts)
        (<= start s e end) (recur (rest fts) start s (cons [e end] invts)))
      (cons [start end] invts)))) 

; XXX make this zone-id specific
; XXX use start, end in query to restrict fts
(defn- populate-par-history-intervals
  [db asset-type-ids start end]
  (sort-by (juxt :start :end)
    (map (fn [[k v]]
           {:start (first k) :end (second k)
            :asset-type-ids (vec
                              (distinct
                                (map :asset-type-id v)))})
      (group-by :interval
        (mapcat (fn [atid]
                  (let [fts (database/search-entities db :par-history-fetch-timestamps
                              [["SOME=" [:asset-type-ids] atid]]
                              {:order-by [[[:start] -1] [[:end] -1]]})]
                    (map #(hash-map :interval % :asset-type-id atid )
                      (get-intervals fts start end))))
          asset-type-ids)))))

; XXX make this zone-id specific
(defn- update-par-history!
  [_ db asset-type-ids start end]
  (io!
    (try
      (let [intvs (populate-par-history-intervals
                    db asset-type-ids start end)]
        (doseq [{:keys [start end asset-type-ids] :as params} intvs]
          (info (str "Updating par-history for: " params))
          (let [t1 (System/currentTimeMillis)
                rows-count (form-and-persist-par-history!
                             db asset-type-ids start end)]
            (let [t2 (System/currentTimeMillis)]
              (update-fetch-ts! db asset-type-ids start end
                (long (inc (/ (- t2 t1) 1000))) rows-count)))))
      (catch Exception e
        (error "Error updating par history" e)))
    nil))

(defn- get-par-history
  [db zone-ids asset-type-ids start end]
  (database/search-entities db :par-history
    [["IN" [:zone-id] zone-ids]
     ["IN" [:asset-type-id] asset-type-ids]
     [">=" [:timestamp] start]
     ["<=" [:timestamp] end]]
    {:order-by [[[:timestamp] 1]]}))

(defn adjust-start-end
  [start end]
  (let [{:keys [firsttimestamp lasttimestamp]} (get-ERC-route-history-info!)]
    [(max start firsttimestamp) (min end lasttimestamp)]))

; history agent is just for serializing history updates
; i.e. its just for synchronization
(defn update-and-get-par-history!
  ([db history-agent zone-ids asset-type-ids start end]
    (let [[start end] (adjust-start-end start end)]
      (when (< end start)
        (throw (IllegalArgumentException.
                 (str "end:" end " cannot be less than start:" start))))
      (when (< (- end start) *par-data-granularity*)
        (throw (IllegalArgumentException.
                 (str "Cannot query with range less than par-data-granularity:"
                   *par-data-granularity*))))
      (when (populate-par-history-intervals
              db asset-type-ids start end)
        (send-off history-agent
          (bound-fn [_]
            (update-par-history!
              _ db asset-type-ids start end)))
        (await history-agent))
      (get-par-history
        db zone-ids asset-type-ids start end))))
