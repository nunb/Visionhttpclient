(ns ekahau.vision.asset-service
  (:require
   [ekahau.vision.database :as database]
   [ekahau.vision.todolist-service :as todolist-service]
   ekahau.engine.cmd)
  (:use
   [clojure.contrib.trace :only [trace]]
   [ekahau.string
    :only [parse-id parse-number]]
   [ekahau.vision.model.asset
    :only [create-get-property-type-by-id-fn]]
   [ekahau.vision.predicate
    :only [create-asset-predicate]]
   [ekahau.vision.model.positions
    :only [get-building-from-map-id]]
   [ekahau.vision.model.zone-grouping
    :only [create-zone-group-entries-by-zone-id-map]]))

(defn- get-predicate-for-asset-specification
  [db asset-spec-id]
  (when asset-spec-id
    (when-let [spec (database/get-entity-by-id db :asset-specs asset-spec-id)]
      (create-asset-predicate db spec))))

(defn- get-assets-filtered-by-spec
  [db asset-spec-id]
  (when-let [pred (get-predicate-for-asset-specification db asset-spec-id)]
    (set (filter pred (database/get-entities db :assets)))))

(defn- get-assets-by-spec
  [db asset-spec-id]
  (or
   (when asset-spec-id
     (get-assets-filtered-by-spec db asset-spec-id))
   (set (database/get-entities db :assets))))

(defn- get-set-of-asset-type-parent-ids
  [asset-types]
  (set (remove nil? (map :parent-type-id asset-types))))

(defn- get-asset-type-ids-including-ancestors
  [asset-types asset-type-ids]
  (loop [result-set asset-type-ids
         current-level-asset-types (filter (comp asset-type-ids :id)
                                           asset-types)]
    (let [new-ids (clojure.set/difference
                   (get-set-of-asset-type-parent-ids asset-types)
                   result-set)]
      (if (empty? new-ids)
        result-set
        (recur (clojure.set/union result-set new-ids)
               (filter (comp new-ids :id) current-level-asset-types))))))

(defn get-asset-types-and-ancestors
  [db asset-type-ids]
  (let [asset-types (database/get-entities db :asset-types)
        id-set (get-asset-type-ids-including-ancestors asset-types
                                                       asset-type-ids)]
    (->> asset-types
         (filter (comp id-set :id))
         (set))))

(defn asset-types-of-assets
  [db assets]
  (get-asset-types-and-ancestors
   db
   (->> assets
        (map :asset-type-id)
        (set))))

(defn create-assets-report-from-assets
  [db assets]
  {:asset-types (asset-types-of-assets db assets)
   :assets assets})

(defn create-assets-report-from-spec
  [db asset-spec-id]
  (create-assets-report-from-assets
   db (get-assets-by-spec db asset-spec-id)))

(defn tag-information-from-tag
  [db tag]
  {:serialNumber (:serialnumber tag)
   :mac (:mac tag)
   :type (:type tag)
   :batteryLevel (parse-number (:battery tag))})

(defn add-asset-tag-information
  [db assets]
  (let [engine-asset-ids (remove nil? (map :engine-asset-id assets))]
    (let [tags
          (if-let [engine-asset-ids (seq
                                     (remove nil?
                                             (map :engine-asset-id assets)))]
           (ekahau.engine.cmd/epe-get-tags
            {:fields "all"
             :assetid engine-asset-ids}))
          tags-by-engine-asset-id
          (->> tags
               (map :properties)
               (map (comp (juxt (comp parse-id :assetid) identity)))
               (into {}))]
      (map
       (fn [asset]
         (if-let [tag (get tags-by-engine-asset-id (:engine-asset-id asset))]
           (assoc asset
             :tag-information (tag-information-from-tag db tag))
           asset))
       assets))))

(defn zone-group-information-from-entries
  [zone-group-entries]
  (map
   (fn [{{grouping-id :id} :zone-grouping
         {group-id :id group-name :name} :zone-group}]
     {:groupingId grouping-id
      :group {:id group-id
              :name group-name}})
   zone-group-entries))

(defn position-information-from-position-observation
  [db
   zone-groups-by-zone-id
   {{:keys [timestamp] {:keys [map-id zone-id point]} :position}
    :position-observation}]
  (let [model-map (database/get-entity-by-id db :maps map-id)
        building (get-building-from-map-id db map-id)
        zone (database/get-entity-by-id db :zones zone-id)]
    {:timestamp timestamp
     :position {:building {:id (:id building)
                           :name (:name building)}
                :map {:id map-id
                      :name (:name model-map)}
                :mapPoint point
                :zone {:id zone-id
                       :name (:name zone)}
                :zoneGroups (zone-group-information-from-entries
                             (zone-groups-by-zone-id zone-id))}}))

(defn add-asset-position-information
  [db assets]
  (let [zone-groups-by-zone-id (create-zone-group-entries-by-zone-id-map db)]
   (map
    (fn [asset]
      (let [position-observation
            (database/get-entity-by-id db
                                       :asset-position-observations
                                       (:id asset))]
        (assoc asset :position-information
               (position-information-from-position-observation
                db zone-groups-by-zone-id position-observation))))
    assets)))

(defn add-asset-extra-info
  [assets-report db]
  (update-in
   assets-report [:assets]
   (fn [assets]
     (->> assets
          (add-asset-position-information db)
          (add-asset-tag-information db)))))

(defn add-asset-to-do-list-info
  [assets-report db to-do-list-id]
  (if-let [to-do-list (and to-do-list-id (database/get-entity-by-id db :todo-lists to-do-list-id))]
    (update-in
     assets-report [:assets]
     todolist-service/add-asset-item-information to-do-list-id (:items to-do-list))
    assets-report))

(defn asset-report-with-extra-info
  [db asset-spec-id to-do-list-id]
  (-> (create-assets-report-from-spec db asset-spec-id)
      (add-asset-extra-info db)
      (add-asset-to-do-list-info db to-do-list-id)))
