(ns ekahau.reports
  (:require
    [ekahau.engine.connection]
    [ekahau.engine.cmd :as engine.cmd])
  (:use
    [clojure.contrib.str-utils :only (str-join)]
    ekahau.math
    [ekahau.string :only [parse-int parse-long]]
    [ekahau.util :only (assoc-multi mac-to-long)]
    [ekahau.vision.database :only (position position-observation route)]
    [ekahau.vision.model.map :only (area-includes?)]))

(defn- parse-position-observation
  [emsg]
  (let [timestamp (parse-long (:timestamp emsg))
        map-id (parse-long (:mapid emsg))
        zone-id (parse-long (:zoneid emsg))
        x (parse-int (:x emsg))
        y (parse-int (:y emsg))]
    (-> (struct position-observation (struct position map-id [x y]) timestamp)
      (assoc-in [:position :zone-id] zone-id))))

(defn- assoc-vec
  [m k v]
  (assoc-multi m k v []))

(defn- assoc-veccat
  [m k v]
  (assoc m k (into (get m k []) v)))

;; Old Reporting ;;

(defn- parse-routes
  [response-route-points]
  (reduce
    #(assoc-vec %1 (first %2) (second %2)) ; Map, Key (tagid), Value (position observation)
    {}
    (map vector
      (map #(Long/parseLong (:tagid %)) response-route-points)
      (map parse-position-observation response-route-points))))

(defn- request-routes
  [tag-ids start-time end-time]
  (let [response (engine.cmd/routehistorybrowse-by-tags tag-ids start-time end-time)
        routes (parse-routes (map :properties response))]
    (map #(struct route (key %) (sort-by :timestamp (val %))) routes)))

(defstruct zone-visit :zone-id :time-interval)
(defstruct zone-event :zone-id :timestamp :crossing-type)

(defn- convert-event-from-ratio-to-time
  [id time-interval event]
  (struct zone-event id (long (interpolate time-interval (:ratio event))) (:crossing-type event)))

(defn- get-polygon-line-segment-intersection-events-with-time
  "Parameters: [zone [route-point-1 route-point-2]]"
  [{id :id {polygon :polygon} :area} [{{p1 :point} :position start :timestamp} {{p2 :point} :position end :timestamp}]]
  (let [events (get-polygon-line-segment-intersection-events polygon [p1 p2])
        time-interval [start end]]
    (map (partial convert-event-from-ratio-to-time id time-interval) events)))

(defn- enter-event-for-route-starting-from-zone
  [zone route]
  (if (seq route)
    (if (area-includes? (:area zone) (-> route first :position))
      [(struct zone-event (:id zone) (-> route first :timestamp) :enter)])))

(defn get-polygon-route-intersection-events-with-time
  [zone route]
  (concat
    (enter-event-for-route-starting-from-zone zone route)
    (mapcat (partial get-polygon-line-segment-intersection-events-with-time zone) (partition 2 1 route))))

(defn convert-route-intersection-events-to-visits
  ([events]
    (lazy-seq
      (when-let [event (first events)]
        (condp = (:crossing-type event)
          :enter
          (convert-route-intersection-events-to-visits (rest events) (:timestamp event))
          :exit
          (convert-route-intersection-events-to-visits (rest events))
          :passing
          (convert-route-intersection-events-to-visits (rest events))))))
  ([events enter-time]
    (lazy-seq
      (when-let [event (first events)]
        (condp = (:crossing-type event)
          :enter
          (convert-route-intersection-events-to-visits (rest events) enter-time)
          :exit
          (cons
            (struct zone-visit (:zone-id event) [enter-time (:timestamp event)])
            (convert-route-intersection-events-to-visits (rest events)))
          :passing
          (convert-route-intersection-events-to-visits (rest events) enter-time))))))

(defn get-route-visits-on-zone
  [route zone]
  (let [events (get-polygon-route-intersection-events-with-time zone route)]
    (convert-route-intersection-events-to-visits events)))

(defn- get-all-route-visits-on-zone
  "Returns a map that has asset-id keys and route visit sequece values."
  [routes zone]
  (reduce #(assoc-veccat %1 (:tag-id %2) (get-route-visits-on-zone (:position-observations %2) zone)) {} routes))

(defn- create-asset-visits-on-zone-xml
  [asset-visits-on-zone]
  (into
    [:asset {:id (key asset-visits-on-zone)}]
    (map
      (fn [{[start end] :time-interval}] [:visit {:start (long start) :end (long end)}])
      (val asset-visits-on-zone))))

(defn- create-zone-visit-xml
  [zone-visits]
  (into [:zone {:id (-> zone-visits :zone :id)}] (map create-asset-visits-on-zone-xml (:route-visits zone-visits))))

(defstruct asset-count-entry :timestamp :count)

(defn- before?
  [t1 t2]
  (condp = [(nil? t1) (nil? t2)]
    [true true]   nil
    [false true]  true
    [true false]  false
    [false false] (<= t1 t2)))

(defn- visits-to-tally-events
  [visits]
  (sort-by first (mapcat (fn [{[enter exit] :time-interval}] [[enter inc] [exit dec]]) visits)))

(defn get-asset-counts-over-time
  ([visits]
    (get-asset-counts-over-time visits 0))
  ([visits count]
    (let [all-events (visits-to-tally-events visits)
          f (fn this [events tally]
              (lazy-seq
                (when (seq events)
                  (let [[time tally-fn] (first events)
                        new-tally (tally-fn tally)
                        rest-of-events (rest events)]
                    (if (= (ffirst rest-of-events) time)
                      (this rest-of-events new-tally)
                      (cons
                        (struct asset-count-entry time new-tally)
                        (this rest-of-events new-tally)))))))]
      (f all-events count))))

(defn sum-series
  ([s1 s2]
    (sum-series s1 s2 0 0))
  ([s1 s2 s1v s2v]
    (lazy-seq
      (let [[t1 v1] (first s1)
            [t2 v2] (first s2)]
        (cond
          (before? t1 t2)
          (cons [t1 (+ v1 s2v)] (sum-series (rest s1) s2 v1 s2v))
          (before? t2 t1)
          (cons [t2 (+ s1v v2)] (sum-series s1 (rest s2) s1v v2))
          true
          nil)))))

(defn combine-to-area-chart-dataset
  [series]
  (letfn [(get-sum-series [previous series]
            (lazy-seq
              (when-let [current (first series)]
                (let [a (sum-series previous current)]
                  (cons a (get-sum-series a (rest series)))))))]
    (get-sum-series [] series)))

(defn partition-time-series
  [series dividers]
  (lazy-seq
    (when (and (seq series) (seq dividers))
      (let [[a b] (split-with #(< (:timestamp %) (first dividers)) series)]
        (cons a (partition-time-series b (rest dividers)))))))

(defn- duration
  [[start end]]
  (- end start))

;
; Count statistics
;

(defn average-count
  [initial-count series [start-time end-time :as total-interval]]
  (assert (not (nil? initial-count)))
  (let [total-duration (duration total-interval)]
    (reduce +
      (map
        (fn [part-interval count]
          (* count (/ (duration part-interval) total-duration)))
        (partition 2 1 (concat [start-time] (map :timestamp series) [end-time]))
        (cons initial-count (map :count series))))))
;
; Generic count calculation
;

(defn- calculate-asset-counts-from-partitions
  [f series initial-count dividers]
  (assert (not (nil? initial-count)))
  (lazy-seq
    (when (seq series)
      (cons
        (struct asset-count-entry
          (first dividers)
          (double (f initial-count (first series) (take 2 dividers))))
        (calculate-asset-counts-from-partitions f (rest series) (or (-> series first last :count) initial-count) (rest dividers))))))

(defn calculate-asset-counts
  [f series initial-count start-time partitioning]
  (let [dividers (iterate #(+ % partitioning) start-time)
        partitioned-series (partition-time-series series (rest dividers))]
    (calculate-asset-counts-from-partitions f partitioned-series initial-count dividers)))

;
;
;

(defn- format-asset-count-data
  [asset-counts]
  (str-join ";"
    (map
      (fn [values]
        (str-join "," values))
      asset-counts)))

(defn- create-asset-counts-dataset
  [f asset-counts]
  (map
    (fn [{:keys [asset-count-series]}]
      (map (fn [{:keys [timestamp count]}] [timestamp count]) (f asset-count-series)))
    asset-counts))

(defn- create-asset-count-report
  [name f asset-counts]
  (let [dataset (combine-to-area-chart-dataset (create-asset-counts-dataset f asset-counts))]
    [name
     (map
       (fn [{:keys [zone]} data]
         [:zone (select-keys zone [:id :name])
          (format-asset-count-data data)
          ])
       asset-counts
       dataset)
     ]))

;
;
;

(defn get-route-zone-visits
  ([route-points end-time]
    (lazy-seq
      (when (seq route-points)
        (let [current (first route-points)]
          (if (<= 0 (or (-> current :position :zone-id) -1))
            (get-route-zone-visits (rest route-points) (-> current :position :zone-id) (:timestamp current) end-time)
            (get-route-zone-visits (rest route-points) end-time))))))
  ([route-points current-zone-id zone-enter-timestamp end-time]
    (lazy-seq
      (if (seq route-points)
        (let [{{zone-id :zone-id} :position timestamp :timestamp} (first route-points)]
          (if (not (= current-zone-id zone-id))
            (cons
              (struct zone-visit current-zone-id [zone-enter-timestamp timestamp])
              (if (<= 0 zone-id)
                (get-route-zone-visits (rest route-points) zone-id timestamp end-time)
                (get-route-zone-visits (rest route-points) end-time)))
            (get-route-zone-visits (rest route-points) current-zone-id zone-enter-timestamp end-time)))
        (cons
          (struct zone-visit current-zone-id [zone-enter-timestamp end-time])
          (get-route-zone-visits route-points end-time))))))

(defn- get-visits-per-zone-id-from-routes
  [routes end-time]
  (reduce
    (fn [m {zone-id :zone-id :as visit}]
      (assoc m zone-id (conj (get m zone-id []) visit)))
    {}
    (mapcat #(get-route-zone-visits (:position-observations %) end-time) routes)))

(defstruct zone-asset-counts :zone :asset-count-series)

(defn get-zone-asset-counts-over-time-from-routes
  [routes zones end-time]
  (map
    (fn [[zone-id visits]]
      (if-let [zone (first (filter #(= zone-id (:id %)) zones))]
        (struct zone-asset-counts zone (get-asset-counts-over-time visits))))
    (get-visits-per-zone-id-from-routes routes end-time)))

(defn- get-zone-asset-counts
  [tag-ids start-time end-time zones]
  (let [routes (request-routes tag-ids start-time end-time)]
    (get-zone-asset-counts-over-time-from-routes routes zones end-time)))

(defn create-asset-counts-per-zone-over-time
  [tag-ids start-time end-time zones]
  (let [bin-duration (* 1000 60 15)
        zone-asset-counts (get-zone-asset-counts tag-ids start-time end-time zones)
        calculate (fn [series] (calculate-asset-counts average-count series 0 start-time bin-duration))]
    [:report
     (create-asset-count-report :averageCounts calculate zone-asset-counts)
     ]))
