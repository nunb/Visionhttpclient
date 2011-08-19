(ns ekahau.vision.routes.map
  (:require
   [ekahau.vision.database :as database]
   [ekahau.vision.model.positions :as positions]
   [ekahau.vision.columns :as columns])
  (:use
   [clojure.contrib.str-utils :only [re-split str-join]]
   [clojure.contrib.trace :only [trace]]
   clojure.set
   compojure.core
   [ekahau.vision.model.map :as map]
   [ekahau.vision.model.asset :only (get-short-asset-string
                                     get-asset-by-engine-asset-id)]
   [ekahau.vision.model.positions :only [form-detailed-position-from-zone-id]]
   [ekahau.vision.predicate :only [create-asset-predicate
                                   get-asset-predicate-by-spec-id]]
   [ekahau.vision.routes.helpers :only
    [get-user-by-session
     parse-request-route-id
     to-json
     with-png-content-type
     with-xml-content-type
     with-json-content-type
     create-page-not-found-response]]
   [ekahau.vision.xml.map :only [map-xml]]
   [ekahau.vision.xml.asset-type :only [asset-types-xml]]
   [ekahau.string :only [parse-id]]
   [ekahau.util :only [assoc-if select-keys-with-non-nil-values]]
   [ekahau.xml :only [to-xml-str]]))


(defn get-asset-position-entry-by-position-observation
  [db position-observation]
  (when-let [asset (database/get-entity-by-id db
                                              :assets
                                              (:id position-observation))]
    [asset (:position-observation position-observation)]))

(defn asset-position-observation-entries-on-map
  [db map-id]
  (->> (positions/get-asset-position-observations-on-map db map-id)
       (map (partial get-asset-position-entry-by-position-observation db))
       (remove nil?)))

(defn assoc-satisfies-specification
  [pred asset result]
  (if pred
    (assoc result :satisfiesSpecification (pred asset))
    result))

(defn create-selected-asset-types-asset-specification
  [db user]
  (let [{:keys [asset-types]} (columns/get-site-view-columns db user)]
    {:type "or"
     :specs (map
             (fn [asset-type]
               {:type "asset-type"
                :asset-type-id asset-type})
             asset-types)}))

(defn get-assets-on-map-json
  [db map-id asset-spec-id user]
  (let [pred (create-asset-predicate
              db
              (create-selected-asset-types-asset-specification db user))
        assoc-satisfies-specification
        (partial assoc-satisfies-specification
                 (get-asset-predicate-by-spec-id db asset-spec-id))]
    (->> (asset-position-observation-entries-on-map db map-id)
         (filter (fn [[asset _]] (pred asset)))
         (map
          (fn [[{:keys [id asset-type-id] :as asset}
               {:keys [timestamp]
                {point :point} :position}]]
            (assoc-satisfies-specification
             asset
             {:id id
              :assetTypeId asset-type-id
              :positionObservation
              {:timestamp timestamp
               :point point}})))
         (to-json))))

(defn- assoc-matches-spec
  [asset pred]
  (if pred
    (assoc asset :matches-spec (pred asset))
    asset))

(defn- map-asset-xml
  [{id :id, name :name, asset-type-id :asset-type-id, position-observation :position-observation, matches-spec :matches-spec}]
  (let [[x y] (-> position-observation :position :point)
        timestamp (:timestamp position-observation)]
    [:asset 
     (assoc-if
       {:id id, :name name, :timestamp timestamp, :x x, :y y, :assetType asset-type-id}
       (complement nil?)
       :matchesSpec matches-spec)]))

(defn- map-assets-xml
  [assets]
  [:assets (map map-asset-xml assets)])

(defn- polygon-to-string
  [polygon]
  (str-join ";" (map #(str-join "," %) polygon)))

(defn- zone-polygon-to-string
  [zone]
  (polygon-to-string (-> zone :area :polygon)))

(defn- zone-xml
  [zone]
  [:zone
   (assoc
     (select-keys zone [:id :name])
     :uri (str "zones/" (:id zone))
     :polygon (zone-polygon-to-string zone))])

(defn- zones-xml
  [zones]
  [:zones (map zone-xml zones)])

(defn- zones-xml-from-db
  [db map-id]
  (zones-xml (filter #(-> % :area :map-id (= map-id)) (database/get-entities db :zones))))

(defn- view-map
  [db id asset-spec-id]
  (when-let [a-map (database/get-entity-by-id db :maps id)]
    [:view-map {:id id}
     (map-xml a-map)
     (zones-xml-from-db db id)
     (asset-types-xml (database/get-entities db :asset-types))
     (let [asset-pred (get-asset-predicate-by-spec-id db asset-spec-id)]
       (->> (map/get-assets-with-position-observations db id)
         (map
           (fn [asset]
             (-> asset
               (assoc :name (get-short-asset-string db asset))
               (assoc-matches-spec asset-pred))))
         (sort-by #(-> % :position-observation :timestamp))
         (map-assets-xml)))]))

(defn- map-ids-from-request
  [request]
  (->> (-> request :params :*)
    (re-split #";")
    (map parse-id)))

(defn parse-asset-spec-id
  [request]
  (parse-id (-> request :params :assetSpec)))

(defn- do-get-maps
  [db request]
  (let [map-ids (map-ids-from-request request)]
    (if (and (= 1 (count map-ids)) (not= "true" (-> request :params :brief)))
      (let [id (first map-ids)
            asset-spec-id (parse-asset-spec-id request)]
        (or
          (to-xml-str (view-map db id asset-spec-id))
          (create-page-not-found-response)))
      (to-xml-str
        (into [:maps]
          (->> map-ids
            (map #(database/get-entity-by-id db :maps %))
            (map #(vector :map (select-keys % [:id :name])))))))))

(defn create-map-routes
  [db]
  (routes
   (->
    (GET "/maps/:id.png" {:as request}
         (let [id (parse-request-route-id request)
               a-map (database/get-entity-by-id db :maps id)
               image (:image a-map)]
           (if image
             @image
             (create-page-not-found-response))))
    (with-png-content-type))
   (->
    (GET "/maps/:id/assets.json" {session :session
                                  :as request}
         (let [user (get-user-by-session db session)]
           (get-assets-on-map-json
            db
            (parse-request-route-id request)
            (parse-asset-spec-id request)
            user)))
    (with-json-content-type))
   (->
    (routes
     (GET "/zones/:zid.json" [zid]
          (to-json
           (select-keys-with-non-nil-values
             (form-detailed-position-from-zone-id db zid)))))
    (with-json-content-type))
   (->
    (routes
     (GET "/maps/:id/zones" {:as request}
          (let [id (parse-request-route-id request)]
            (to-xml-str (zones-xml-from-db db id))))
     (GET "/maps/*" {:as request} (do-get-maps db request)))
    (with-xml-content-type))))
