(ns ekahau.vision.event-report-service
  (:require
    [ekahau.vision.event-service :as es]
    [ekahau.vision.database :as database])
  (:use
    [clojure.contrib.logging :only [warn]]
    [ekahau.math :only [get-max-min-avg]]
    [ekahau.time :only [number-of-days-since-epoch]]
    [ekahau.vision.event-rule-service :only [get-event-rule-by-id]]))

(defn- get-max-min-avg-events-in-a-day
  [events {:keys [max-timestamp min-timestamp]}]
  (let [event-timestamps (map :timestamp events)
        [max-timestamp-1 min-timestamp-1 _] (get-max-min-avg event-timestamps)
        [max-timestamp min-timestamp] [(or max-timestamp max-timestamp-1)
                                       (or min-timestamp min-timestamp-1)]
        [max-day min-day] (map number-of-days-since-epoch
                            [max-timestamp min-timestamp])
        freq-event-days (frequencies
                          (map number-of-days-since-epoch
                            event-timestamps))
        freq-event-days (into {}
                          (map #(vector % (get freq-event-days % 0))
                            (range min-day (inc max-day))))]
    [max-timestamp min-timestamp (inc (- max-day min-day))
     (get-max-min-avg (vals freq-event-days))]))

(defn- get-max-min-avg-time-events-remain-open
  [events]
  (let [event-open-millis (map #(- (:closing-time %) (:timestamp %))
                            (filter :closing-time events))]
    (get-max-min-avg event-open-millis)))

(def my-comparator
  (let [convert-map-to-vec (juxt :number-of-events :id :version :name)]
    #(let [c (compare
               (convert-map-to-vec %1)
               (convert-map-to-vec %2))]
       (when (= c 0)
         (warn (str "my-comparator for event-report returned 0 for objs\n" %1 "\n" %2)))
       c)))

(defn- get-frequencies-report-1
  ([db events ks f]
    (sort my-comparator
      (map (fn [[k v]] (into k {:number-of-events v}))
        (frequencies
          (map #(f % ks) events)))))
  ([db events ks]
    (get-frequencies-report-1 db events ks get-in)))

(defn- get-building-frequencies
  [db events]
  (get-frequencies-report-1 db events
    [:position-observation :position :building]))

(defn- get-floor-frequencies
  [db events]
  (get-frequencies-report-1 db events
    [:position-observation :position :floor]))

(defn- get-zone-frequencies
  [db events]
  (get-frequencies-report-1 db events
    [:position-observation :position :zone]))

(defn- get-event-rule-frequencies
  [db events]
  (get-frequencies-report-1 db events
    [:event-rule-info]))

(defn- get-in-2
  [db use-latest? event ks]
  (get-in (if use-latest?
            (es/get-latest-event-details db event)
            event)
    ks))

(defn- get-asset-type-frequencies
  [db events {:keys [use-latest-asset-types?]}]
  (get-frequencies-report-1 db events [:asset-type-info]
    (partial get-in-2 db use-latest-asset-types?)))

(defn- get-zone-group-frequencies
  [db events {:keys [use-latest-zone-groupings?]}]
  (sort my-comparator
    (map (fn [[k v]]
           (into k {:zone-groups
                    (sort my-comparator
                      (map (fn [[k v]]
                             (into (:zone-group k)
                               {:number-of-events v}))
                        (let [freq-zgs (frequencies v)
                              sum-zgs (apply + (vals freq-zgs))]
                          (if (= sum-zgs (count events))
                            freq-zgs
                            (assoc freq-zgs {:zone-group {:id nil :name nil}}
                              (- (count events) sum-zgs))))))}))
      (group-by :zone-grouping
        (mapcat #(get-in-2 db use-latest-zone-groupings? %
                   [:position-observation :position :zone-grouping])
          events)))))

(defn- sanitize-number
  [n]
  (when n
    (float (/ (long (* 10 n)) 10))))

(defn- generate-report
  [db events opts]
  (when (seq events)
    (let [num-events (count events)
          [max-timestamp min-timestamp num-days [mad mid avgd]]
          (get-max-min-avg-events-in-a-day events opts)
          [mao mio avgo] (get-max-min-avg-time-events-remain-open events)]
      {:number-of-events num-events
       :max-timestamp max-timestamp
       :min-timestamp min-timestamp
       :number-of-days num-days
       :max-number-of-events-per-day mad
       :min-number-of-events-per-day mid
       :avg-number-of-events-per-day (sanitize-number avgd)
       :max-time-open mao
       :min-time-open mio
       :avg-time-open (sanitize-number avgo)
       :number-of-events-per-building (get-building-frequencies db events)
       :number-of-events-per-floor (get-floor-frequencies db events)
       :number-of-events-per-zone (get-zone-frequencies db events)
       :number-of-events-per-zone-group (get-zone-group-frequencies db events opts)
       :number-of-events-per-asset-type (get-asset-type-frequencies db events opts)
       :number-of-events-per-event-rule (get-event-rule-frequencies db events)})))

(defn generate-report-for-some-event-ids
  [db event-ids opts]
  (let [events (remove nil?
                 (map (partial es/get-event-by-id db) event-ids))]
    (generate-report db events opts)))

(defn generate-report-for-all-events
  ([db opts]
    (generate-report db (es/get-events db) opts))
  ([db]
    (generate-report-for-all-events db {})))
