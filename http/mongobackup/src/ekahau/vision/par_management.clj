(ns ekahau.vision.par-management
  (:require
    [ekahau.vision.database :as database])
  (:use
    [clojure.set :only (union difference)]
    [clojure.string :only [split]]
    [clojure.contrib.logging :only [debug info warn error]]
    [ekahau.vision.par-snapshot :only [get-assets-count
                                       update-par-snapshot]]
    [ekahau.vision.event-service :only [construct-event
                                        handle-event!
                                        event-rule-details]]
    [ekahau.vision.event-rule-service :only [get-event-rules
                                             get-event-rule-by-id
                                             par-management-rule?]]
    [ekahau.vision.model.asset :only [get-asset-by-id
                                      get-asset-types
                                      get-asset-type-and-descendants]]
    [ekahau.vision.model.positions :only [get-building-from-map-id
                                          get-map-by-id
                                          get-floor-from-map-id
                                          get-zone-by-id
                                          get-zone-map-id
                                          form-detailed-position-from-zone-id]]))

(defn get-active-par-management-rules
  [db]
  (filter #(and (:active? %) (par-management-rule? %))
    (get-event-rules db)))

(defn get-par-rule-zone-id
  [par-rule]
  (:zone-id (:trigger par-rule)))

(defn- get-par-rule-asset-types
  [db par-rule]
  (->>
   (if-let [spec (-> par-rule :trigger :event-subject-specification)]
     (case (:type spec)
           "all" (get-asset-types db)
           "selected-types" (mapcat (partial get-asset-type-and-descendants db)
                                    (:ids spec)))
     (get-asset-types db))
   (remove nil?)
   (set)))

(defn get-par-rule-asset-type-ids
  [db par-rule]
  (map :id (get-par-rule-asset-types db par-rule)))

(defn- get-num-assets-satisfying-par-rule
  [db rule snapshot]
  (let [zone-id (get-par-rule-zone-id rule)]
    (reduce + (map (partial get-assets-count snapshot zone-id)
                (get-par-rule-asset-type-ids db rule)))))

(defn- construct-pos-obs
  [db par-rule timestamp]
  (struct-map database/position-observation
    :position (form-detailed-position-from-zone-id
                db (get-par-rule-zone-id par-rule))
    :timestamp timestamp))

(defn- construct-event-for-par-management
  [db rule num-assets timestamp]
  (let [event-id (database/get-next-free-entity-id! db :events)]
    (construct-event
      {:id event-id
       :type "vision-event"
       :par-info (let [{{:keys [low-limit high-limit]} :trigger} rule]
                   {:asset-types (map #(select-keys % [:id :name])
                                   (get-par-rule-asset-types db rule))
                    :num-assets num-assets
                    :low-limit  low-limit
                    :high-limit high-limit
                    :problem    (cond
                                  (< num-assets low-limit)  "Lower than low limit"
                                  (> num-assets high-limit) "Higher than high limit")})
       :event-rule-info (event-rule-details rule)
       :position-observation (construct-pos-obs db rule timestamp)
       :timestamp timestamp
       :closed? false})))

(defn- invoke-par-management-rule-action!
  [db rule num-assets timestamp]
  (handle-event! db rule
    (construct-event-for-par-management db rule num-assets timestamp)))

(defn par-rule-triggered?
  [par-manager-state rule]
  (get-in par-manager-state [:triggered (:id rule) (:version rule)] false))

(defn- trigger-par-rule
  [par-manager-state rule]
  (assoc-in par-manager-state [:triggered (:id rule) (:version rule)] true))

(defn- untrigger-par-rule
  [par-manager-state rule]
  (assoc-in par-manager-state [:triggered (:id rule) (:version rule)] false))

(defn- par-rule-asset-count-within-limits?
  [rule asset-count]
  (let [{{:keys [low-limit high-limit]} :trigger} rule]
    (<= low-limit asset-count high-limit)))

(defn- go-through-par-management-rules!
  [db par-manager-state]
  (reduce
   (fn [{:keys [snapshot triggered] :as par-manager-state} rule]
     (let [timestamp (System/currentTimeMillis)
           num-assets (get-num-assets-satisfying-par-rule
                       db rule snapshot)
           rule-disobeyed? (not
                            (par-rule-asset-count-within-limits? rule
                                                                 num-assets))]
       (cond
        (and rule-disobeyed? (not (par-rule-triggered? par-manager-state rule)))
        (do
          (info (str "Par management rule satisfied rule:" (:id rule)
                     " version:" (:version rule) " num-assets="num-assets))
          (invoke-par-management-rule-action! db rule num-assets timestamp)
          (trigger-par-rule par-manager-state rule))
        (and (not rule-disobeyed?) (par-rule-triggered? par-manager-state rule))
        (untrigger-par-rule par-manager-state rule)
        :else par-manager-state)))
   par-manager-state
   (get-active-par-management-rules db)))

(defn- update-par-manager-snapshot
  [db snapshot asset-id new-pos]
  (update-par-snapshot snapshot
    {:assetid     asset-id
     :zoneid      (:zone-id new-pos)
     :assettypeid (:asset-type-id
                    (get-asset-by-id db asset-id))}))

(defn init-par-manager-state
  [_ db]
  {:snapshot (reduce
               #(update-par-manager-snapshot db %1 %2
                  (get-in (database/get-entity-by-id db :asset-position-observations %2)
                    [:position-observation :position]))
               {}
               (map :id (database/get-entities db :assets)))
   :triggered {}})

(defn invoke-par-management!
  [{:keys [snapshot triggered] :as par-manager-state} db asset-id obs]
  (debug (str "Par management for location: asset-id=" asset-id " obs=" obs))
  (try
    (go-through-par-management-rules! db
      (merge par-manager-state
        {:snapshot (update-par-manager-snapshot
                     db snapshot asset-id (:position obs))}))
    (catch Exception e
      (error "Error invoking par management" e)
      (init-par-manager-state nil db))
    (finally
      (debug (str "Par management for location: asset-id=" asset-id " obs=" obs)))))

(defn- par-rule->uri
  [rule]
  (str "par-rule/" (:id rule) "/" (:version rule)))

(defn- convert-par-snapshot-to-par-view
  [sn]
  (let [create-rules
        (fn [values]
          (vec
           (map (fn [[rule _]]
                  {:name (:name rule)
                   :uri (par-rule->uri rule)})
                values)))

        create-floor
        (fn [a-map values]
          {:name (:name a-map)
           :uri (str "maps/" (:id a-map))
           :children (create-rules values)})

        create-floors
        (fn [floors-map]
          (reduce
           (fn [results [a-map values]]
             (conj results (create-floor a-map values)))
           []
           floors-map))

        create-building
        (fn [building floors-map]
          {:name (or (:name building) "No Building")
           :uri (str "buildings/" (or (:id building) "none"))
           :children (create-floors floors-map)})

        create-values
        (fn [rule num-a]
          [{:value num-a
            :withinLimits
            (par-rule-asset-count-within-limits?
             rule
             num-a)}
           (:low-limit (:trigger rule))
           (:high-limit (:trigger rule))])]
    (reduce
     (fn [view [building floors-map]]
       (-> view
           (update-in [:rows :children] conj
                      (create-building building floors-map))
           (update-in [:values]
                      (fn [values]
                        (loop [values (conj values [])
                               floors-seq (seq floors-map)]
                          (if floors-seq
                            (recur
                             (into (conj values [])
                                   (map (fn [[rule num-a]]
                                          (create-values rule num-a))
                                        (second (first floors-seq))))
                             (next floors-seq))
                            values))))))
     {:rows {:name "All" :uri "all" :children []}
      :columns [{:name "Number of items"
                 :uri "count"}
                {:name "Min"
                 :uri "min"}
                {:name "Max"
                 :uri "max"}]
      :values [[]]}
     sn)))

(defn par-site-view-snapshot
  [db {:keys [snapshot triggered] :as par-manager-state} par-rules]
  (convert-par-snapshot-to-par-view
    (reduce
      (fn [sn rule]
        (let [zone-id  (get-par-rule-zone-id rule)
              zone     (get-zone-by-id db zone-id)
              map-id   (get-zone-map-id db (get-zone-by-id db zone-id))
              building (get-building-from-map-id db map-id)
              floor    (get-floor-from-map-id db map-id)
              map      (get-map-by-id db map-id)
              num-a    (get-num-assets-satisfying-par-rule db rule snapshot)]
          (update-in sn [building map] (fnil conj []) [rule num-a])))
      {}
      par-rules)))

(defn flat-snapshot
  [db {:keys [snapshot triggered] :as par-manager-state} par-rules]
  {:columns [{:name "Number of Items" :uri "count"}
             {:name "Min" :uri "min"}
             {:name "Max" :uri "max"}]
   :rows (map
          (fn [rule]
            {:uri (par-rule->uri rule)
             :name (:name rule)
             :values [(let [count (get-num-assets-satisfying-par-rule db rule snapshot)]
                        {:count count
                         :withinLimits
                         (par-rule-asset-count-within-limits?
                          rule
                          count)}) 
                      (:low-limit (:trigger rule))
                      (:high-limit (:trigger rule))]})
          par-rules)})
