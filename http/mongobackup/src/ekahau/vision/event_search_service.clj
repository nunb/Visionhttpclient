(ns ekahau.vision.event-search-service
  (:require
    [ekahau.vision.database :as database]
    [ekahau.vision.event-service :as es])
  (:use
    [ekahau.util :only [filter-search]]
    [clojure.contrib.logging :only [error]]))

(defn- keywordize-keys
  [ks]
  (vec (map #(if (keyword? %) % (keyword %)) ks)))

(defn- form-search-params
  [db search-params convert-ks gs]
  (remove nil?
    (map (fn [[f {:keys [ks] :as params} vs]]
           (let [ks (keywordize-keys ks)]
             (cond
               (contains? gs [ks f]) ((get gs [ks f]) db vs) ; pred-fn / search doc provided
               (contains? convert-ks ks) [f (get convert-ks ks) vs] ; simple form query
               :else (do
                       (error (str "Cannot find a rule to convert " ks " to search param"))))))
      search-params)))

(def form-filter-search-params
  ^{:private true}
   form-search-params)

(def form-in-mem-search-params
  ^{:private true}
   form-filter-search-params)

(def form-mongodb-search-params
  ^{:private true}
   form-search-params)

(def *convert-ks*
  ^{:private true}
  {[:building-id]      [:position-observation :position :building :id]
   [:map-id]           [:position-observation :position :map :id]
   [:zone-id]          [:position-observation :position :zone :id]
   [:event-rule-id]    [:event-rule-info :id]
   [:asset-id]         [:asset-info :id]
   [:asset-type-id]    [:asset-type-info :id]
   [:engine-asset-id]  [:engine-asset-id]
   [:timestamp]        [:timestamp]
   [:closed?]          [:closed?]
   [:zone-grouping-id] [[:position-observation :position :zone-grouping] [:zone-grouping :id]]})

(def *gs-for-use-latest*
  ^{:private true}
  {[[:zone-grouping-id] "SOME-KEY-IN"] (fn [db vs]
                                         (let [ks [:position-observation :position :zone-grouping]
                                               vs (set vs)]
                                           (fn [event]
                                             (some #(contains? vs %)
                                               (map (comp :id :zone-grouping)
                                                 (get-in (es/get-latest-event-details db event [ks]) ks))))))
   [[:asset-type-id] "IN"] (fn [db vs]
                             (let [ks [:asset-type-info]
                                   vs (set vs)]
                               (fn [event]
                                 (contains? vs
                                   (:id (get-in (es/get-latest-event-details db event [ks]) ks))))))})

(defn- qualifies-for-db-search?
  [[f {:keys [ks use-latest?]} vs]]
  (not use-latest?))

(defn- split-search-params
  [search-params]
  (reduce
    (fn [[sp-db sp-f] sp]
      (if (qualifies-for-db-search? sp)
        [(conj sp-db sp) sp-f]
        [sp-db (conj sp-f sp)]))
    [[] []]
    search-params))

; search params are [f {:keys [ks use-latest?]} vs]
(defn search-events
  [db search-params & {:as opts}]
  (let [[search-params-for-db search-params-for-filter] (split-search-params search-params)
        db-search-params (case (database/db-type db :events)
                           :in-memory (form-in-mem-search-params db
                                        search-params-for-db *convert-ks* nil)
                           :mongodb (form-mongodb-search-params db
                                      search-params-for-db *convert-ks* nil))
        filter-search-params (form-filter-search-params db
                               search-params-for-filter *convert-ks* *gs-for-use-latest*)
        db-search-results (database/search-entities db :events
                            db-search-params opts)]
    (if (seq filter-search-params)
      (filter-search filter-search-params db-search-results)
      db-search-results)))
