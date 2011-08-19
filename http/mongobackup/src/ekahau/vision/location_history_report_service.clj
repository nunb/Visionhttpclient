(ns ekahau.vision.location-history-report-service
  (:require
   [ekahau.vision.database :as database]
   [ekahau.event-statistics]
   [clojure.contrib.logging :as logging]
   [ekahau.vision.location-history-report.asset-report :as asset-report]
   [ekahau.vision.location-history-report.location-report :as location-report]
   [ekahau.vision.predicate :as predicate])
  (:use
   [clojure.contrib.trace
    :only [trace]]
   [ekahau.vision.model.asset
    :only [get-short-asset-string]]
   [ekahau.vision.location-history-report.base-report]
   [ekahau.vision.location-history-report.asset-states]
   [ekahau.vision.location-history-report.position-states]
   [ekahau.vision.location-history-report.route-point]
   [ekahau.vision.routes.building
    :only [create-buildings-in-site
           create-zone-groupings-in-site
           rows-from-site-hierarchy
           site-nodes-in-preorder]]))

(defn- get-engine-asset-ids-from-assets
  [assets]
  (->> assets
       (filter :engine-asset-id)
       (map :engine-asset-id)))

(defn- get-engine-asset-ids
  [db asset-pred]
  (->> (database/get-entities db :assets)
       (filter (or asset-pred (constantly true)))
       (get-engine-asset-ids-from-assets)))

(defn- transitive-mapping
  [m1 m2]
  (reduce
   (fn [m [k v]]
     (assoc m k (get m2 v)))
   {} m1))

(defn get-engine-asset-id-to-asset-id-map
  [db]
  (->> (database/get-entities db :assets)
       (remove (comp nil? :engine-asset-id))
       (map (juxt :engine-asset-id :id))
       (into {})))

(defn create-get-states-by-engine-asset-id
  [db column-specs]
  (let [asset-id-to-states
        (create-states-from-asset-fn-from-key-predicate-entries
         db
         (location-report/create-key-predicate-entries-from-column-specs
          db column-specs))
        engine-asset-id-to-asset-id
        (get-engine-asset-id-to-asset-id-map db)]
    (transitive-mapping engine-asset-id-to-asset-id
                        asset-id-to-states)))

(defn get-location-report-states
  [position-to-states engine-asset-id-to-states route-point]
  (let [position-states (position-to-states
                         (get-in route-point [:pos-obs :position]))
        asset-states (engine-asset-id-to-states (:engine-asset-id route-point))]
    (set
     (for [position-state position-states
           asset-state asset-states]
       [position-state asset-state]))))

(defn create-get-location-report-states-fn
  "Creates a function (fn [route-point] ...) that returns a sequence of states
  the route point is associated with. State is a pair
  [location-state asset-state] that can interpreted for example as a cell of
  a table."
  [db column-specs]
  (partial get-location-report-states
           (create-states-from-position-fn db)
           (create-get-states-by-engine-asset-id db
                                                 column-specs)))

(defonce jobs (atom {}))

(defn time-interval-ratio
  [[start end] time-point]
  (/ (- time-point start)
     (- end start)))

(defn- report-progress-by-timestamp
  [db report route-point]
  (trace "progress" (double (time-interval-ratio
                             (:time-interval report)
                             (get-in route-point [:pos-obs :timestamp]))))
  route-point)

(defn route-point-to-statistics-event
  [get-location-report-states
   {asset-id :engine-asset-id {timestamp :timestamp} :pos-obs :as route-point}]
  {:id asset-id
   :states (get-location-report-states route-point)
   :data timestamp})

(defn route-points-to-location-statistics-events
  [db report route-points]
  (let [get-location-report-states
        (create-get-location-report-states-fn db
                                              (:columns-request report))]
    (map (partial route-point-to-statistics-event
                  get-location-report-states)
         route-points)))

(defn get-asset-report-states
  [position-to-states route-point]
  (let [position-states (position-to-states
                         (get-in route-point [:pos-obs :position]))
        asset-states [{:engine-asset-id (:engine-asset-id route-point)}]]
    (set
     (for [asset-state asset-states
           position-state position-states]
       [asset-state position-state]))))

(defn create-get-asset-report-states-fn
  [db]
  (partial get-asset-report-states
           (create-states-from-position-fn db)))

(defn route-points-to-asset-statistics-events
  [db report route-points]
  (let [get-asset-report-states
        (create-get-asset-report-states-fn db)]
    (map (partial route-point-to-statistics-event
                  get-asset-report-states)
         route-points)))

(defonce short-circuited-results (atom nil))
(defonce route-generator-atom (atom nil))

(defn statistics-events-from-location-history
  [db report route-points-to-statistics-events-fn engine-asset-ids]
  (->> (if-let [generate-route @route-generator-atom]
         (generate-route engine-asset-ids
                         (:time-interval report))
         (get-engine-assets-route-points-within-interval
           engine-asset-ids (:time-interval report)))

       (route-points-to-statistics-events-fn db report)))

(defn final-empty-states
  [{[_ end-time]  :time-interval} engine-asset-ids]
  (map (fn [id]
         {:id id
          :states #{}
          :data end-time})
       engine-asset-ids))

(defn get-report-engine-asset-ids
  [db report]
  []
  (->> (database/get-entities db :assets)
       (map :engine-asset-id)
       (remove nil?)))

(defn all-statistics-events-from-location-history
  [db report route-points-to-statistics-events-fn engine-asset-ids]
  (concat (statistics-events-from-location-history
           db
           report
           route-points-to-statistics-events-fn
           engine-asset-ids)
          (final-empty-states report engine-asset-ids)))

(defn done-jobs
  [job-map]
  (map key (filter #(future-done? (val %)) job-map)))

(defn remove-done-jobs!
  []
  (future
   (swap! jobs (fn [jobs-value]
                 (apply dissoc jobs-value (done-jobs jobs-value))))))

(defn get-value-of-state
  [statistics state-key statistic-key]
  (get-in statistics [statistic-key state-key]))

(def statistic-report-cell-keys
     [[:visit-count :visit-count-per-state]
      [:min-concurrent-visitors :min-concurrent-visitors]
      [:max-concurrent-visitors :max-concurrent-visitors]
      [:total-visitors :visitors-per-state count]
      [:average-visit-time :average-visit-time-per-state double]
      [:average-total-time :average-total-time-per-state double]
      [:total-visit-time :total-visit-time-per-state]
      [:non-visitor-count :entities-not-visited count]])

(defn- get-statistics-report-cell-value-entries
  [statistics row-key column-key]
  (let [get-value (partial get-value-of-state statistics [row-key column-key])]
    (map (fn [[_ k & fs]]
           (when-let [v (get-value k)]
             (if-let [fseq (seq fs)]
               ((apply comp fs) v)
               v)))
         statistic-report-cell-keys)))

(defn report-cell-value-from-entries
  [entries]
  (when-not (every? nil? entries)
    (->>
     (map
      (fn [[k _ & _] v]
        [k v])
      statistic-report-cell-keys entries)
     (into {}))))

(defn- default-non-visitor-count
  [statistics column-key]
  (count (get-in statistics
                 [:entity-asset-ids-per-asset-key
                  column-key]
                 [])))

(def constantly-zero (constantly 0))

(defn row-can-have-values?
  [row-key]
  (not
   (or (nil? row-key)
       (= row-key {:buildings :all})
       (contains? row-key :zone-grouping))))

(defn ensure-defaults
  [result statistics row-key column-key]
  (if (row-can-have-values? row-key)
   (let [stuff [[:non-visitor-count
                 (partial default-non-visitor-count statistics column-key)]
                [:min-concurrent-visitors constantly-zero]
                [:max-concurrent-visitors constantly-zero]
                [:total-visitors constantly-zero]
                [:total-visit-time constantly-zero]
                [:visit-count constantly-zero]]]
     (reduce
      (fn [result [k f]]
        (if-not (contains? result k)
          (assoc result k (f))
          result))
      result stuff))
   result))

(defn ensure-non-visitor-count
  [result statistics row-key column-key]
  (if-not (contains? result :non-visitor-count)
    (assoc result
      :non-visitor-count
      (when-let [a (get-in statistics [:entity-asset-ids-per-asset-key
                                       column-key])]
        (count a)))
    result))

(defn get-statistics-report-cell-value
  [statistics [row-key column-key]]
  (-> (get-statistics-report-cell-value-entries statistics
                                                row-key
                                                column-key)
      (report-cell-value-from-entries)
      (ensure-defaults statistics
                       row-key
                       column-key)))

(defn add-entity-asset-ids-per-asset-key
  [statistics db column-specs entity-ids]
  (let [get-states (create-get-states-by-engine-asset-id db
                                                         column-specs)]
    (assoc statistics
      :entity-asset-ids-per-asset-key
      (reduce
       (fn [result engine-asset-id]
         (let [states (get-states engine-asset-id)]
           (reduce
            (fn [result state]
              (update-in result [state] (fnil conj #{}) engine-asset-id))
            result states)))
       {} entity-ids))))

(defn create-location-statistics
  [db report]
  (let [{[start-time end-time] :time-interval} report]
    (binding [ekahau.event-statistics.steps/initial-event?
              (fn [init-data data]
                (= data start-time))
              ekahau.event-statistics.steps/get-event-data-timestamp
              (fn [data]
                data)
              ekahau.event-statistics.steps/closing-event?
              (fn [init-data data]
                (= data end-time))]
      (when-let [engine-asset-ids (seq (get-report-engine-asset-ids db report))]
        (-> (all-statistics-events-from-location-history
             db
             report
             route-points-to-location-statistics-events
             engine-asset-ids)
            (ekahau.event-statistics/create-report engine-asset-ids)
            (add-entity-asset-ids-per-asset-key
             db
             (:column-request report)
             (set engine-asset-ids)))))))

(defn create-asset-statistics
  [db report engine-asset-ids]
  (let [{[start-time end-time] :time-interval} report]
    (binding [ekahau.event-statistics.steps/initial-event?
              (fn [init-data data]
                (= data start-time))
              ekahau.event-statistics.steps/get-event-data-timestamp
              (fn [data]
                data)
              ekahau.event-statistics.steps/closing-event?
              (fn [init-data data]
                (= data end-time))]
      (-> (all-statistics-events-from-location-history
           db
           report
           route-points-to-asset-statistics-events
           engine-asset-ids)
          (ekahau.event-statistics/create-asset-report)))))

(defn create-location-report-result
  [db report]
  (let [statistics (create-location-statistics db report)]
    (location-report/get-buildings-with-column-specifications
      db
      (:columns-request report)
      (partial get-statistics-report-cell-value
               statistics))))

(defn get-asset-report-column-value
  [statistics row-key column]
  (into
   {}
   (map
    (fn [[result-key stats-key]]
      [result-key
       (when-let [value (get-in statistics [stats-key [row-key (:key column)]])]
         (if (instance? clojure.lang.Ratio value)
           (double value)
           value))])
    [[:visit-count :visit-count-per-state]
     [:average-visit-time :average-visit-time-per-state]
     [:total-visit-time :total-visit-time-per-state]])))

(defn create-asset-report-column-value
  [statistics row-key column]
  (into
   {:value (get-asset-report-column-value
            statistics
            row-key
            column)}
   (map
    (fn [child-column]
      [(:uri child-column)
       (create-asset-report-column-value statistics row-key child-column)])
    (:children column))))

(defn create-asset-report-result-values
  [statistics assets columns]
  (map
   (fn [asset]
     (let [k {:engine-asset-id (:engine-asset-id asset)}]
       (map
        (partial create-asset-report-column-value statistics k)
        columns)))
   (conj assets nil)))

(defn create-asset-report-result
  [db report]
  (let [pred (if-let [spec (:selected-assets report)]
               (predicate/create-asset-predicate db spec)
               (constantly true))
        assets (filter pred (database/get-entities db :assets))
        engine-asset-ids (get-engine-asset-ids-from-assets assets)
        statistics (create-asset-statistics db report engine-asset-ids)
        columns (concat
                 (create-buildings-in-site db)
                 (create-zone-groupings-in-site db))]
    {:rows {:uri "assets"
            :name "Assets"
            :children
            (map
             (fn [asset]
               {:name (get-short-asset-string db asset)
                :uri (format "assets/%s" (:id asset))})
             assets)}
     :columns (map rows-from-site-hierarchy columns)
     :values (create-asset-report-result-values statistics assets columns)}))

(defn complete-report
  [db report]
  (let [report (prepare-report report)]
    (-> report
        (assoc
            :result (condp = (get report :type "location")
                        "location" (create-location-report-result db report)
                        "asset" (create-asset-report-result db report))
            :status "complete")
        (dissoc :progress))))

(defn failed-report
  [report]
  (-> report
      (assoc :status "failed")
      (dissoc :progress)))

(defn run-report
  [db report]
  (logging/info (format "Running report: id=%s name=\"%s\""
                        (:id report) (:name report)))
  (try
    (let [result (complete-report db report)]
      (logging/info (format "Report completed: id=%s name=\"%s\""
                            (:id report) (:name report)))
      result)
    (catch Throwable t
      (logging/error "Report failed!" t)
      (failed-report report))))

(defn run-report!
  [db report]
  (try
    (database/put-entity! db :reports
                          (run-report db report))
    (catch Throwable t
      (logging/error "Saving report failed!" t))
    (finally
     (remove-done-jobs!))))

(defn- start-report-job!
  [db report]
  (let [f (bound-fn [] (run-report! db report))]
    (future
     (f))))

(defn start-report!
  [db report]
  (swap! jobs assoc {:report-id (:id report)} (start-report-job! db report)))
