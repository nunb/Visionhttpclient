(ns ekahau.vision.routes.site
  (:require
   [ekahau.UID :as uid]
   [ekahau.vision.columns :as columns]
   [ekahau.vision.database :as database]
   ekahau.vision.model.building
   [ekahau.vision.json.asset :as json-asset]
   [ekahau.vision.asset-service :as asset-service]
   [ekahau.vision.uri :as uri])
  (:use
   [clojure.contrib.trace :only [trace]]
   compojure.core
   [ekahau.vision.routes.helpers :only [get-user-by-session
                                        to-json
                                        with-json-content-type]]
   [ekahau.vision.site-service :only [create-get-asset-count-cell-value-fn
                                      create-get-asset-count-cell-value-of-specific-assets]]))

(defn row-values
  "Returns a sequence of values on row defined be row-key."
  [get-cell-value row-key column-keys]
  (map
   #(get-cell-value [row-key %])
   column-keys))

(defn create-column-keys-of-type
  "Returns a column key of given type for each id in id-coll."
  [type id-coll]
  (map (fn [id] {type id}) id-coll))

(defn create-column-keys
  "Returns a collection of column keys based on columns."
  [{asset-type-ids :asset-types
    asset-spec-ids :asset-specs
    include-all? :include-all?
    include-total? :include-total?
    :or {include-all? true
         include-total? true}}]
  (concat
   (when include-all?
     [{:asset-spec :all}])
   (mapcat
    (fn [[type ids]] (create-column-keys-of-type type ids))
    [[:asset-type asset-type-ids]
     [:asset-spec asset-spec-ids]])
   (when include-total?
     [{:asset-spec :total}])))

(defn uri-for-key
  [key id]
  (case key
        :buildings "all"
        :building (uri/building-uri id)
        :map (uri/map-uri id)
        :zone (uri/zone-uri id)))

(defn create-row-entry
  [db column-keys get-cell-value key entity]
  (assoc (select-keys entity [:id :name])
    :uri (uri-for-key key (:id entity))
    :values (row-values get-cell-value {key (:id entity)}
                        column-keys)))

(defn create-response
  [db user row-values key asset-spec-id total-row total-key]
  (let [columns (assoc (columns/get-site-view-columns db user)
                  :include-total? true)]
    (to-json
     {:columns (columns/create-report-columns db columns)
      :rows
      (let [get-cell-value (create-get-asset-count-cell-value-fn db
                                                                 columns
                                                                 asset-spec-id)
            column-keys (create-column-keys columns)]
        (concat
         (map (partial create-row-entry db column-keys get-cell-value key) row-values)
         [(assoc (create-row-entry db column-keys get-cell-value total-key total-row)
            :name "Total")]))})))

;; ## Site

(defn create-site-response
  [db user asset-spec-id]
  (create-response db
                   user
                   (sort-by :name (database/get-entities db :buildings))
                   :building
                   asset-spec-id
                   {:id :all}
                   :buildings))

;; ## Building

(defn get-maps-of-building
  [db building-id]
  (map #(database/get-entity-by-id db :maps (:map-id %))
       (reverse (ekahau.vision.model.building/get-sorted-floors-of-building db building-id))))

(defn create-building-response
  [db id user asset-spec-id]
  (create-response db
                   user
                   (get-maps-of-building db id)
                   :map
                   asset-spec-id
                   (database/get-entity-by-id db :buildings id)
                   :building))

;; ## Map

(defn get-zones-of-map
  [db id]
  (database/search-entities db :zones
                            [["=" [:area :map-id] id]]
                            {:order-by [[[:name] 1]]}))

(defn create-map-response
  [db id user asset-spec-id]
  (create-response db
                   user
                   (get-zones-of-map db id)
                   :zone
                   asset-spec-id
                   (database/get-entity-by-id db :maps id)
                   :map))

;; ## Site Zone Groupings

(defn building-map-id-set
  [db building-id]
  (->> (database/get-entities db :floors)
       (filter #(= building-id (:building-id %)))
       (map :map-id)
       (set)))

(defn get-asset-id-set-of-assets-on-maps
  [db map-id-set]
  (->>
   (database/get-entities db :asset-position-observations)
   (filter #(map-id-set (get-in % [:position-observation :position :map-id])))
   (map :id)
   (set)))

(defn get-asset-id-set-of-assets-in-building
  [db building-id]
  (get-asset-id-set-of-assets-on-maps db (building-map-id-set db building-id)))

(defn get-asset-id-set-of-assets-on-map
  [db map-id]
  (get-asset-id-set-of-assets-on-maps db #{map-id}))

(defn create-asset-in-building-pred
  [db building-id]
  (comp (get-asset-id-set-of-assets-in-building db building-id) :id))

(defn create-asset-on-map-pred
  [db map-id]
  (comp (get-asset-id-set-of-assets-on-map db map-id) :id))

(defn create-zone-groupings-response
  [db user pred asset-spec-id]
  (let [assets (filter pred (database/get-entities db :assets))
        columns (assoc (columns/get-site-view-columns db user)
                  :include-total? true)]
    (to-json
     {:columns (columns/create-report-columns db columns)
      :rows
      (let [get-cell-value (create-get-asset-count-cell-value-of-specific-assets
                            db
                            columns
                            assets
                            asset-spec-id)
            column-keys (create-column-keys columns)]
        (map
         (fn [zone-grouping]
           {:name (:name zone-grouping)
            :uri (uri/zone-grouping-uri (:id zone-grouping))
            :values (repeat (count column-keys) nil)
            :children
            (map
             (fn [zone-group]
               (assoc (select-keys zone-group [:id :name])
                 :uri (uri/zone-group-uri (:id zone-grouping) (:id zone-group))
                 :values (row-values get-cell-value
                                     {:zone-grouping (:id zone-grouping)
                                      :zone-group (:id zone-group)}
                                     column-keys)))
             (:groups zone-grouping))})
         (database/get-entities db :zone-groupings)))})))

;; ## All Routes

(defn create-site-routes
  [db]
  (->
   (routes
    (GET "/site.json" {{asset-spec-id-str :assetSpec} :params
                       session :session}
         (let [user (get-user-by-session db session)]
           (create-site-response db user (uid/parse-id asset-spec-id-str))))
    (GET "/site/buildings/:id.json" {{asset-spec-id-str :assetSpec
                                      :keys [id]} :params
                                      session :session}
         (let [user (get-user-by-session db session)]
           (create-building-response db id user (uid/parse-id asset-spec-id-str))))
    (GET "/site/maps/:id.json" {{:keys [id]
                                 asset-spec-id-str :assetSpec} :params
                                 session :session}
         (let [user (get-user-by-session db session)]
           (create-map-response db id user (uid/parse-id asset-spec-id-str))))
    (GET "/site/zoneGroupings.json"
         {{asset-spec-id-str :assetSpec} :params
          session :session}
         (let [user (get-user-by-session db session)]
           (create-zone-groupings-response db user
                                           (constantly true)
                                           (uid/parse-id asset-spec-id-str))))
    (GET "/site/buildings/zoneGroupings/:id.json"
         {{:keys [id]
           asset-spec-id-str :assetSpec} :params
           session :session}
         (let [user (get-user-by-session db session)]
           (create-zone-groupings-response db user
                                           (create-asset-in-building-pred db (uid/parse-id id))
                                           (uid/parse-id asset-spec-id-str))))
    (GET "/site/maps/zoneGroupings/:id.json"
         {{:keys [id]
           asset-spec-id-str :assetSpec} :params
           session :session}
         (let [user (get-user-by-session db session)]
           (create-zone-groupings-response db user
                                           (create-asset-on-map-pred db (uid/parse-id id))
                                           (uid/parse-id asset-spec-id-str)))))
   (with-json-content-type)))
