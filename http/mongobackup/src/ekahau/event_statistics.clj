(ns ekahau.event-statistics
  (:require [ekahau.event-statistics.steps :as steps])
  (:use [ekahau.event-statistics.report :only
         [create-reporter-with-calculators]]))

(def entity-states-step
     [:entity-states {}
      steps/update-entity-states])

(def visit-starts-step
     [:visit-starts {}
      steps/update-visit-starts
      [:entity-states]])

(def completed-visits-step
     [:completed-visits []
      steps/update-completed-visits
      [:visit-starts :entity-states]])

(def visitors-per-state-step
     [:visitors-per-state {}
      steps/update-report-visitors-per-state
      []])

(def visit-count-per-state-step
     [:visit-count-per-state {}
      steps/update-visit-count-per-state
      [:completed-visits]])

(def total-visit-time-per-state-step
     [:total-visit-time-per-state {}
      steps/update-total-visit-time-per-state
      [:completed-visits]])

(def min-concurrent-visitors-step
     [:min-concurrent-visitors {}
      steps/update-min-concurrent-visitors
      [:init-data :visit-starts :completed-visits]])

(def max-concurrent-visitors-step
     [:max-concurrent-visitors {}
      steps/update-max-concurrent-visitors
      [:visit-starts]])

(def create-report-from-events
     (create-reporter-with-calculators
       [entity-states-step
        visit-starts-step
        completed-visits-step]
       [visitors-per-state-step
        visit-count-per-state-step
        total-visit-time-per-state-step
        min-concurrent-visitors-step
        max-concurrent-visitors-step]))

(defn create-report
  ([events]
     (create-report events []))
  ([events entity-ids]
     (-> (create-report-from-events events)
         (steps/update-average-visit-time-per-state)
         (steps/update-average-total-time-per-state)
         (steps/update-not-visited-in entity-ids))))

(def create-asset-report-from-events
     (create-reporter-with-calculators
       [entity-states-step
        visit-starts-step
        completed-visits-step]
       [visit-count-per-state-step
        total-visit-time-per-state-step]))

(defn create-asset-report
  ([events]
     (create-asset-report events []))
  ([events entity-ids]
     (-> (create-asset-report-from-events events)
         (steps/update-average-visit-time-per-state))))