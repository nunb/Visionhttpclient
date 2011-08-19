(ns ekahau.event-statistics.steps)

(declare get-event-data-timestamp initial-event? closing-event?)

;; Entity states

(defn update-entity-states
  [result {:keys [id states]}]
  (assoc result id states))

;; Visit starts

(defn clean-visits
  [result {:keys [id states]} entity-states]
  (reduce
   (fn [result state]
     (let [o (dissoc (get result state) id)]
       (if (empty? o)
         (dissoc result state)
         (assoc result state o))))
   result (clojure.set/difference
           (get entity-states id)
           states)))

(defn visit-started?
  [result state id]
  (get-in result [state id]))

(defn start-visit-in-state
  [result state id data]
  (assoc-in result [state id] data))

(defn start-new-visits
  [result {:keys [id states data]}]
  (reduce
   (fn [result state]
     (if-not (visit-started? result state id)
       (start-visit-in-state result state id data)
       result))
   result states))

(defn update-visit-starts
  [result event entity-states]
  (-> result
      (clean-visits event entity-states)
      (start-new-visits event)))

;; Latest completed visits

(defn update-completed-visits
  [_ {:keys [id states data]} visit-starts entity-states]
  (map
   (fn [old-state]
     (let [start (get-in visit-starts [old-state id])]
       {:id id
        :state old-state
        :enter-data start
        :exit-data data}))
   (clojure.set/difference
    (get entity-states id)
    states)))

;; Visitor per state (all seen visitors)

(defn add-states
  [result id states]
  (reduce
   (fn [result state]
     (update-in result [state] (fnil conj #{}) id))
   result states))

(defn update-report-visitors-per-state
  [report {:keys [id states]}]
  (add-states report id states))

;; Visit count per state

(defn update-visit-count-per-state
  [result _ completed-visits]
  (reduce
   (fn [result {state :state}]
     (update-in result [state] (fnil inc 0)))
   result completed-visits))

;; Total visit time per state

(defn get-visit-duration
  [{:keys [enter-data exit-data]}]
  (- (get-event-data-timestamp exit-data)
     (get-event-data-timestamp enter-data)))

(defn update-total-visit-time-per-state
  [result _ completed-visits]
  (reduce
   (fn [result {state :state :as visit}]
     (update-in result [state] (fnil + 0) (get-visit-duration visit)))
   result completed-visits))

;; Average visit time per state

(defn update-average-visit-time-per-state
  [report]
  (let [{:keys [visit-count-per-state
                total-visit-time-per-state]} report]
    (assoc report
      :average-visit-time-per-state
      (->> (keys total-visit-time-per-state)
           (map (fn [state]
                  [state (/ (get total-visit-time-per-state state)
                            (get visit-count-per-state state))]))
           (into {})))))

;; Average total time per state

(defn update-average-total-time-per-state
  [report]
  (let [{:keys [total-visit-time-per-state
                visitors-per-state]} report]
    (assoc report
      :average-total-time-per-state
      (->> (keys total-visit-time-per-state)
           (map (fn [state]
                  (let [visitor-count (count (get visitors-per-state state))]
                    (if (pos? visitor-count)
                      [state (/ (get total-visit-time-per-state state)
                                visitor-count)]
                      0))))
           (into {})))))

;; Minimum concurrent visitors

(defn- zero-minimum-when-non-initial-enter
  [result event]
  (reduce
   (fn [result state]
     (update-in result [state]
                (fnil min 0)))
   result (:states event)))

(defn- check-minimum-on-exit
  [result visit-starts completed-visits]
  (reduce
   (fn [result {state :state}]
     (update-in result [state]
                (fn [value] (min value
                                 (count (get visit-starts state))))))
   result completed-visits))

(defn- increase-minimum
  [result event visit-starts]
  (reduce
   (fn [result state]
     (update-in result [state] (fnil max 0)
                (count (get visit-starts state))))
   result (:states event)))

(defn update-min-concurrent-visitors
  [result event init-data visit-starts completed-visits]
  (cond
   (initial-event? init-data (:data event))
   (increase-minimum result event visit-starts)

   (closing-event? init-data (:data event))
   result

   :else
   (-> result
       (zero-minimum-when-non-initial-enter event)
       (check-minimum-on-exit visit-starts completed-visits))))

;; Maximum concurrent visitors

(defn update-max-concurrent-visitors
  [result {states :states} visit-starts]
  (reduce
   (fn [result state]
     (update-in result [state] (fnil max 0) (count (get visit-starts state))))
   result states))

;; Not visited in state

(defn update-not-visited-in
  [result entity-ids]
  (assoc result
    :entities-not-visited
    (let [entity-id-set (set entity-ids)]
      (->> (:visitors-per-state result)
           (map
            (fn [[state entities]]
              [state (clojure.set/difference entity-id-set
                                             entities)]))
           (into {})))))