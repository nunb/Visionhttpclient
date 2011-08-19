(ns ekahau.vision.routes.asset
  (:require
   [clojure.java.io :as io]
   [ekahau.engine.connection]
   [ekahau.engine.cmd]
   [ekahau.vision.asset-service :as asset-service]
   [ekahau.vision.database :as database]
   [ekahau.vision.engine-service :as engine-service]
   [ekahau.vision.location-history-report.route-point :as route-pts]
   [ekahau.vision.location-service]
   [ekahau.vision.json.asset :as json-asset]
   [ekahau.vision.json.asset-spec :as json-asset-spec]
   [ekahau.vision.predicate :as predicate])
  (:use
   clojure.set
   [clojure.contrib.logging
    :only [error]]
   [clojure.contrib.json
    :only [read-json]]
   [clojure.contrib.seq
    :only [find-first]]
   [clojure.contrib.str-utils
    :only [str-join]]
   [clojure.contrib.trace
    :only [trace]]
   [clojure.xml
    :only [parse]]
   compojure.core
   [ring.util.response :only [redirect]]
   [ekahau.string
    :only [parse-id parse-number parse-long]]
   [ekahau.vision.routes.helpers
    :only [create-database-entity-from-xml!
           get-database-entity-by-route-id
           parse-id-string
           parse-request-route-id
           to-json
           with-xml-content-type
           with-json-content-type
           create-resource-not-found-response
           create-page-not-found-response]]
   [ekahau.vision.model.asset
    :only [get-asset-property
           get-asset-type-and-ancestors
           get-all-asset-types
           get-asset-by-engine-asset-id
           get-type-of-asset-property
           parse-asset-property-value
           get-asset-type-property-paths-from-asset-types
           create-get-property-type-by-id-fn
           find-assets-by-property-key
           get-asset-type-property-groups
           get-short-asset-string
           get-asset-by-id
           get-asset-type-card]]
   [ekahau.vision.xml.tag
    :only [create-tag-element]]
   [ekahau.vision.xml.asset
    :only [create-asset-element
           create-asset-elements
           format-property]]
   [ekahau.vision.xml.asset-type
    :only [asset-type-xml]]
   [ekahau.util
    :only [select-keys-with-non-nil-values]]
   [ekahau.xml
    :only [get-elements-of-type
           to-xml-str]]))

;; Read assets ;;

(defn- create-property-id-to-property-path-map
  [db]
  (create-get-property-type-by-id-fn (database/get-entities db :asset-types)))

(defn- create-elements-for-assets
  [db assets]
  (create-asset-elements assets
                         (create-property-id-to-property-path-map db)))

(defn create-asset-type-elements
  [db asset-types]
  (map
    (fn [asset-type]
      (asset-type-xml asset-type
                      (get-asset-type-property-groups db (:id asset-type))))
    asset-types))

(defn get-asset-spec-query-param
  [request]
  (-> request :params :assetSpec parse-id))

(defn read-assets
  [db request]
  (println "reading assets!!")
  (let [assets-report (asset-service/create-assets-report-from-spec
                       db
                       (get-asset-spec-query-param request))]
    (to-xml-str
     (into [:assets]
           (concat
            (create-asset-type-elements db (:asset-types assets-report))
            (create-asset-elements
             (:assets assets-report)
             (create-get-property-type-by-id-fn
              (:asset-types assets-report))))))))

(defn- get-assets-json
  [db request]
  (to-json
   (json-asset/assets-and-types-of-report-for-json
    db
    (asset-service/asset-report-with-extra-info
      db
      (get-asset-spec-query-param request)
      (-> request :params :toDoList parse-id)))))

(defn- post-assets-set-json
  [db request]
  (let [body (read-json (io/reader (:body request)))]
    (to-json
     (asset-service/create-assets-report-from-assets
      db
      (map
       #(database/get-entity-by-id db :assets %)
       (map parse-id body))))))

(defn- post-assets-report-json
  [db json]
  (let [pred (->> (json-asset-spec/parse-json-asset-spec (:assetSpec json))
                  (predicate/create-asset-predicate db))]
    (to-json
     (json-asset/assets-and-types-of-report-for-json
      db
      (let [result
            (asset-service/add-asset-extra-info
             (asset-service/create-assets-report-from-assets
              db
              (filter pred (database/get-entities db :assets)))
             db)]
        (if-let [to-do-list-id (parse-id (:toDoListId json))]
          (asset-service/add-asset-to-do-list-info
           result db to-do-list-id)
          result))))))

(defn get-all-values-of-property
  [assets property-key]
  (->> assets
       (mapcat :properties)
       (filter #(= property-key (first %)))
       (map #(get % 1))
       (set)))

(defn asset-property-key-from-request
  [request]
  (let [route-params (:params request)]
    (->> [:asset-type-id :property-group-id :property-id]
         (map #(-> route-params % parse-id))
         (vec))))

(defn get-asset-property-values-json
  [db request]
  (to-json
   (sort
    (get-all-values-of-property
     (database/get-entities db :assets)
     (asset-property-key-from-request request)))))

;; Parse asset ;;

(defn- get-value-type
  [asset-types [type-id group-id property-id]]
  (when-let [asset-type (first (filter #(= type-id (:id %)) asset-types))]
    (when-let [group (find-first #(= group-id (:id %))
                                 (:property-groups asset-type))]
      (when-let [property (find-first #(= property-id (:id %))
                                      (:properties group))]
        (:type property)))))

(defn- parse-asset-properties
  [xml asset-types]
  (->> (get-elements-of-type xml :property)
       (map
        (fn [{{id-string :id value-string :value} :attrs}]
          (let [[type-id group-id property-id] (parse-id-string id-string)]
            [[type-id group-id property-id]
             (parse-asset-property-value
               (get-value-type asset-types [type-id group-id property-id])
               value-string)])))))

(defn- parse-asset
  [{{id-str :id asset-type-id-str :assetTypeId} :attrs content :content} db]
  (let [id (parse-id id-str)
        asset-type-id (parse-id asset-type-id-str)
        asset-types (get-all-asset-types db asset-type-id)
        properties (parse-asset-properties content asset-types)]
    (struct database/asset id asset-type-id properties)))

(defn- parse-new-asset
  [xml db]
  (database/assoc-new-entity-id! db :assets (parse-asset xml db)))

;; Update asset ;;

(defn assoc-engine-asset-id-from-existing-asset
  [db new-asset]
  (let [old-asset (database/get-entity-by-id db :assets (:id new-asset))
        engine-asset-id (:engine-asset-id old-asset)]
    (assoc new-asset :engine-asset-id engine-asset-id)))

(defn- do-update-asset
  [db xml]
  (dosync
   (let [asset (->> (parse-asset xml db)
                    (assoc-engine-asset-id-from-existing-asset db))]
      (database/put-entity! db :assets asset))))

;; Get tag of asset ;;

(defn- get-engine-tag-by-asset-id
  [engine-asset-id]
  (->
   (ekahau.engine.cmd/epe-get-tags {:fields "all" :assetid engine-asset-id})
   (first)))

(defn- get-asset-tag
  [db request]
  (let [asset-id (parse-request-route-id request)
        asset (get-asset-by-id db asset-id)]
    (if-let [engine-asset-id (:engine-asset-id asset)]
      (or
        (when-let [tag-emsg (get-engine-tag-by-asset-id engine-asset-id)]
          (to-xml-str (create-tag-element tag-emsg (constantly asset))))
        (create-page-not-found-response))
      (create-page-not-found-response))))

;; Asset/Tag binding ;;

(defn- create-engine-asset-name-from-vision-asset
  "Creates a name for an engine asset based on Vision asset properties"
  [db vision-asset]
  (or (get-short-asset-string db vision-asset) "Vision Asset"))

(defn- get-existing-unused-engine-asset-id
  [db tag-id]
  (let [current-tag-asset-id (engine-service/get-tag-asset-id tag-id)]
    (when-not (get-asset-by-engine-asset-id db current-tag-asset-id)
      current-tag-asset-id)))

(defn- get-asset-type-engine-asset-group-ids
  [db id]
  (->> (get-asset-type-and-ancestors db id)
    (map :engine-asset-group-id)
    (remove nil?)))

(defn- dispatch-add-asset-to-groups
  [db engine-asset-id asset]
  (send-off engine-service/asset-type-asset-group-sync-agent
    (bound-fn [s]
      (try
        (ekahau.engine.cmd/add-asset-to-groups engine-asset-id
          (get-asset-type-engine-asset-group-ids db (:asset-type-id asset)))
        (catch Throwable t
          (error
           "Error adding engine asset to Engine asset type asset groups" t)))
      s)))

(defn- engine-asset-id-for-vision-asset!
  [db asset tag-id]
  (or
   (get-existing-unused-engine-asset-id db tag-id)
   (engine-service/create-asset!
    (create-engine-asset-name-from-vision-asset db asset))))

(defn- assign-engine-asset-id-for-vision-asset!
  "Assigns an engine asset id to the given asset. The engine asset id is one
  that is already bound to the given tag or a newly created one if none
  already exists."
  [db asset tag-id]
  (let [engine-asset-id (engine-asset-id-for-vision-asset! db asset tag-id)]
    (dosync
     (database/put-entity!
      db
      :assets (assoc asset :engine-asset-id engine-asset-id))
      (dispatch-add-asset-to-groups db engine-asset-id asset))
    engine-asset-id))

(defn- get-or-create-engine-asset!
  "Returns an engine asset id that is already stored in the specified
  Vision asset or if one does not exist, a new engine asset is created."
  [db asset-id tag-id]
  (if-let [asset (get-asset-by-id db asset-id)]
    (or
      (:engine-asset-id asset)
      (assign-engine-asset-id-for-vision-asset! db asset tag-id))))

(defn bind-tag-to-asset!
  "Binds an engine tag to the Vision asset specified by asset-id. An engine
  asset is created if necessary."
  [db tag-id asset-id]
  (if-let [engine-asset-id (get-or-create-engine-asset! db asset-id tag-id)]
    (do
      (engine-service/bind-tag-to-asset! tag-id engine-asset-id)
      (let [taginfo (get-engine-tag-by-asset-id engine-asset-id)]
        (database/update-entity! db :assets asset-id assoc
          :tag-id (-> taginfo :properties :tagid)
          :mac (-> taginfo :properties :mac)))
      (when-let [obs (engine-service/get-latest-position-observation-by-tag-id tag-id)]
        (ekahau.vision.location-service/update-asset-position-observation!
          db asset-id obs))
      true)))

(defn unbind-tag-from-asset!
  [db asset-id]
  (if-let [engine-asset-id (:engine-asset-id (get-asset-by-id db asset-id))]
    (do
      (engine-service/unbind-tag-from-asset! engine-asset-id)
      (database/update-entity! db :assets asset-id assoc
        :tag-id nil :mac nil)
      true)))

;; Set tag of asset ;;

(defn- set-asset-tag
  [db request body]
  (let [tag-id (parse-id (-> body :attrs :id))
        asset-id (parse-request-route-id request)]
    (if (bind-tag-to-asset! db tag-id asset-id)
      (to-xml-str [:tag])
      (create-page-not-found-response))))

;; Unset tag of asset ;;

(defn- unset-asset-tag
  [db request body]
  (let [asset-id (parse-request-route-id request)]
    (if (unbind-tag-from-asset! db asset-id)
      (to-xml-str [:tag])
      (create-page-not-found-response))))

;; Get asset ;;

(defn- create-asset-element-by-id
  [db id]
  (let [asset (database/get-entity-by-id db :assets id)]
    (create-asset-element asset (create-property-id-to-property-path-map db))))

(defn- get-asset
  [db request]
  (when-let [id (parse-request-route-id request)]
    (to-xml-str (create-asset-element-by-id db id))))

(defn- create-asset-card-value-attributes
  [asset property-id-to-property-path-map get-property-path-by-key key]
  (let [type (property-id-to-property-path-map key)]
    {:value (format-property (get-asset-property asset key) type)
     :type (name type)
     :label (:label (get (get-property-path-by-key key) 2))
     :key (str-join "," key)}))

(defn- create-get-property-path-by-key-fn
  [asset-types]
  (let [paths (get-asset-type-property-paths-from-asset-types asset-types)]
    (into {} (map (fn [path] [(vec (map :id path)) path]) paths))))

(defn- create-attributes-for-key-fn
  [db asset]
  (let [asset-type-id (:asset-type-id asset)
        asset-types (get-all-asset-types db asset-type-id)]
    (partial create-asset-card-value-attributes
             asset
             (create-get-property-type-by-id-fn asset-types)
             (create-get-property-path-by-key-fn asset-types))))

(defn asset-type-card-of-request
  [db request]
  (when-let [id (parse-request-route-id request)]
    (when-let [asset (database/get-entity-by-id db :assets id)]
      (assoc (get-asset-type-card db (:asset-type-id asset))
        :id id
        :attributes-for-key (create-attributes-for-key-fn db asset)))))

(defn- get-asset-card-json
  [db request]
  (when-let [asset-type-card (asset-type-card-of-request db request)]
    (let [{:keys [id
                  title-key
                  subtitle-keys
                  field-key-rows
                  attributes-for-key]} asset-type-card]
      (to-json {:assetId id
                :title (attributes-for-key title-key)
                :subtitles (map
                            (fn [k]
                              (attributes-for-key k))
                            subtitle-keys)
                :rows (map
                       (fn [row]
                         (map
                          (fn [field]
                            (attributes-for-key field))
                          row))
                       field-key-rows)}))))

(defn- get-asset-card
  [db request]
  (when-let [asset-type-card (asset-type-card-of-request db request)]
    (let [{:keys [id
                  title-key
                  subtitle-keys
                  field-key-rows
                  attributes-for-key]} asset-type-card]
      (to-xml-str
          [:assetCard {:assetId id}
           [:title (attributes-for-key title-key)]
           (map (fn [k] [:subtitle (attributes-for-key k)]) subtitle-keys)
           (map (fn [row]
                  [:row
                   (map (fn [k] [:element (attributes-for-key k)]) row)])
             field-key-rows)]))))

;; Create asset ;;

(defn- create-asset
  [db request]
  (let [body (parse (:body request))
        new-asset (create-database-entity-from-xml! db
                                                    body
                                                    :assets
                                                    parse-new-asset)]
    (to-xml-str (create-asset-element-by-id db (:id new-asset)))))

;; Search assets ;;

(defn- search-assets-req
  [db request]
  (let [body (parse (:body request))]
    (if-let [text (-> body :attrs :text)]
      (let [asset-type-id (parse-id (-> body :attrs :assetTypeId))
            property-group-id (parse-id (-> body :attrs :propertyGroupId))
            property-id (parse-id (-> body :attrs :propertyId))
            property-path [asset-type-id property-group-id property-id]
            results (find-assets-by-property-key
                     (database/get-entities db :assets) property-path text)]
        (to-xml-str [:assets (create-elements-for-assets db results)]))
      (create-page-not-found-response))))

;; Update asset ;;

(defn- update-asset
  [db request body]
  (let [updated-asset (do-update-asset db body)]
    (to-xml-str (create-asset-element-by-id db (:id updated-asset)))))

;; Delete asset ;;

(defn- dispatch-remove-engine-asset-from-groups
  [db asset]
  (send-off
   engine-service/asset-type-asset-group-sync-agent
   (bound-fn [s]
     (try
       (ekahau.engine.cmd/remove-asset-from-groups
        (:engine-asset-id asset)
        (get-asset-type-engine-asset-group-ids db (:asset-type-id asset)))
       (catch Throwable e
         (error "Error removing asset from groups" e)))
     s)))

(defn- delete-asset
  [db id]
  (dosync
    (let [asset (database/get-entity-by-id db :assets id)]
      (database/delete-entity! db :assets id)
      (dispatch-remove-engine-asset-from-groups db asset)))
  (to-xml-str [:asset {:id id}]))

(defn- get-asset-route-point-history
  [db id [since until :as time-interval] route-point-count]
  (when-let [asset (database/get-entity-by-id db :assets id)]
    (when-let [engine-asset-id (:engine-asset-id asset)]
      (cond
        (and until since)
        (route-pts/get-engine-asset-route-points-within
          engine-asset-id time-interval)

        until
        (route-pts/get-engine-asset-route-points-until
          engine-asset-id until route-point-count)

        since
        (route-pts/get-engine-asset-route-points-since
          engine-asset-id since route-point-count)

        :else
        (route-pts/get-latest-engine-asset-route-points
          engine-asset-id route-point-count)))))

(defn- get-position-observation-by-asset-id
  [db asset-id]
  (database/get-entity-by-id db :asset-position-observations asset-id))

(defn- position-observation->xml-attributes-map
  [{{{:keys [map-id zone-id model-id] [x y] :point} :position
     timestamp :timestamp}
    :position-observation}]
  (select-keys-with-non-nil-values
    {:timestamp timestamp
     :modelId model-id
     :mapId map-id
     :zoneId zone-id
     :x x
     :y y}))

(defn get-asset-position
  [db request]
  (or
    (when-let [id (parse-request-route-id request)]
      (when-let [position-observation
                 (get-position-observation-by-asset-id db id)]
        (to-xml-str
         [:asset (merge {:id id}
                        (position-observation->xml-attributes-map
                         position-observation))])))
    (create-resource-not-found-response request)))

(defn- get-asset-route
  [db request]
  (or
    (when-let [id (parse-request-route-id request)]
      (let [params (:params request)
            since (-> params :since parse-number)
            until (-> params :until parse-number)
            route-point-count (or (-> params :count parse-number) 50)]
        (to-xml-str
         (into [:routePoints
                (into {:assetId id :uri (format "assets/%s/history" id)}
                      (remove nil? [(when since {:since since})
                                    (when until {:until until})
                                    (when count {:count route-point-count})]))]
               (map
                (fn [{{{:keys [map-id] [x y] :point} :position
                       :keys [timestamp]} :pos-obs}]
                  [:routePoint {:mapId map-id
                                :x x
                                :y y
                                :timestamp timestamp}])
                (get-asset-route-point-history db
                                               id
                                               [since until]
                                               route-point-count))))))
    (create-resource-not-found-response request)))

(defn- send-message-to-tag-of-asset!
  [db request]
  (when-let [asset (get-database-entity-by-route-id db :assets request)]
    (when-let [engine-asset-id (:engine-asset-id asset)]
      (when-let [tag-id (engine-service/get-asset-tag-id engine-asset-id)]
        (let [body (parse (:body request))]
          (engine-service/send-text-message!
           [tag-id]
           (apply str (:content body)))
          {:status 201})))))

;; Routes ;;

(defn create-asset-routes
  ([db]
     (create-asset-routes db (ref #{})))
  ([db imports-ref]
     (println "here is where all the valid urls are defined in create-asset-routes")
     (routes
      (->
       (routes
        (GET "/assets.json" {:as request} (get-assets-json db request))
        (GET "/assets/:id/card.json" {:as request} (get-asset-card-json db request))
        (POST "/assets/set.json" {:as request} (post-assets-set-json db request))
        (POST "/assets/report.json" {request-body :body}
              (let [body (read-json (io/reader request-body))]
                (post-assets-report-json db body)))
        (GET (str "/assets/propertyValues/"
                  ":asset-type-id/:property-group-id/:property-id.json")
          {:as request}
          (get-asset-property-values-json db request)))
       (with-json-content-type))

      (->
       (routes
        (GET "/assets" {:as request} (read-assets db request))

        (GET "/assets/:id.png" {:as request}
          (when-let [asset (get-database-entity-by-route-id db :assets request)]
            (when-let [asset-type (database/get-entity-by-id
                                   db
                                   :asset-types
                                   (:asset-type-id asset))]
              (redirect (str"/icons/" (:icon asset-type))))))

        (GET "/assets/:id/tag" {:as request} (get-asset-tag db request))

        (POST "/assets/:id/tag" {:as request}
          (let [body (parse (:body request))]
            (condp = (-> body :attrs :_method)
                "PUT" (set-asset-tag db request body)
                "DELETE" (unset-asset-tag db request body)
                (create-page-not-found-response))))

        (POST "/assets/:id/tagMessage" {:as request}
          (send-message-to-tag-of-asset! db request))

        (GET "/assets/:id/card" {:as request} (get-asset-card db request))

        (GET "/assets/:id/position" {:as request} (get-asset-position db request))

        (GET "/assets/:id/route" {:as request} (get-asset-route db request))

        (GET "/assets/:id" {:as request} (get-asset db request))

        (POST "/assets" {:as request} (create-asset db request))

        (POST "/assets/search" {:as request} (search-assets-req db request))

        (POST "/assets/:id" {:as request}
          (let [id (parse-request-route-id request)
                body (parse (:body request))]
            (condp = (-> body :attrs :_method)
                "PUT" (update-asset db request body)
                nil)))

        (PUT "/assets/:id" {:as request}
          (let [id (parse-request-route-id request)
                body (parse (:body request))]
            (update-asset db request body)))

        (DELETE "/assets/:id" {:as request}
          (delete-asset db (parse-request-route-id request))))

       (with-xml-content-type)))))
