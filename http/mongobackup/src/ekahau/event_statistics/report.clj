(ns ekahau.event-statistics.report)

(defn create-reporter-steps
  [specs]
  (for [[k initial-value f & opts] specs]
    (fn [result previous-total event]
      (apply update-in
             result [k]
             f event (map #(% previous-total) (first opts))))))

(defn create-reporter-initial-state
  [specs]
  (into {}
        (for [[k initial-value & _] specs]
          [k initial-value])))

(defn update-report-state-using-steps
  [steps state event]
  (reduce
   (fn [r f]
     (f r state event))
   state steps))

(defn create-update-report-state-fn
  [specs]
  (partial update-report-state-using-steps
           (create-reporter-steps specs)))

(defn create-reporter
  [specs]
  (fn [events]
    (reduce
     (create-update-report-state-fn specs)
     (create-reporter-initial-state specs)
     events)))

(defn create-reporter-with-calculators
  [analysis-specs synthesis-specs]
  (fn this
    ([events]
       (this events nil))
    ([events init-data]
       (let [analyze (create-update-report-state-fn analysis-specs)
             synthesize (create-update-report-state-fn synthesis-specs)]
         (reduce
          (fn [state event]
            (synthesize (analyze state event) event))
          (merge
           {:init-data init-data}
           (create-reporter-initial-state analysis-specs)
           (create-reporter-initial-state synthesis-specs))
          events)))))
