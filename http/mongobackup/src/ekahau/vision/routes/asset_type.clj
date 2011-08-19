(ns ekahau.vision.routes.asset-type
  (:require
   [clojure.java.io :as io]
   [clojure.contrib.json :as json]
   [ekahau.engine.connection]
   [ekahau.vision.database :as database]
   [ekahau.vision.grouping :as grouping]
   [ekahau.vision.json.asset-spec :as json-asset-spec])
  (:use
   clojure.set
   [clojure.xml :only [parse]]
   [clojure.contrib.logging
    :only [error]]
   [clojure.contrib.trace
    :only [trace]]
   compojure.core
   [ring.util.response :only [redirect]]
   [ekahau.engine.cmd :only [delete-asset-group]]
   [ekahau.predicates
    :only [every-pred? value-by-key-in-set-pred?]]
   [ekahau.string
    :only [parse-id parse-number]]
   [ekahau.util
    :only [hash-map-with-non-nil-values key-from-property-path]]
   [ekahau.vision.engine-service
    :only [assoc-new-empty-asset-group-to-asset-type!]]
   [ekahau.vision.json.asset-type
    :only [create-asset-type-to-json-fn]]
   [ekahau.vision.predicate
    :only [create-asset-predicate create-asset-on-map-predicate]]
   ekahau.vision.routes.helpers
   [ekahau.vision.model.asset
    :only [create-ancestor-asset-types-map
           get-asset-property
           find-asset-type-property
           get-type-of-asset-property-from-property-groups
           get-asset-type-property-paths-from-asset-types
           get-asset-type-property-groups
           get-assets-of-type-and-descendants
           get-asset-type-card
           create-is-descendant-of-type-asset-predicate
           get-asset-type-property-values
           get-asset-type-value-satistics
           get-asset-type-and-ancestors
           get-asset-type-and-descendants
           assign-property-group-ids]]
   [ekahau.vision.xml.asset
    :only [asset-property-type-to-xml
           create-asset-elements
           format-property]]
   [ekahau.vision.xml.asset-type
    :only [asset-type-xml xml->card card->xml]]
   [ekahau.vision.xml.helper
    :only [get-xml-attributes]]
   [ekahau.xml
    :only [get-elements-of-type to-xml-str]]))

(defn- fill-asset-type-ids
  [db asset-type]
  (let [assoc-id (partial database/assoc-new-entity-id! db :asset-types)]
    (-> asset-type
      (assoc-id)
      (update-in [:property-groups] assign-property-group-ids))))

(defn- parse-property-group
  [{{id-string :id name-string :name} :attrs content :content}]
  (struct database/property-group (parse-id id-string) name-string
    (map
      (fn [{{id-string :id :keys [label type]} :attrs}]
        (struct database/property
                (parse-id id-string)
                label
                (database/name-to-db-type type)))
      (get-elements-of-type content :property))))

(defn- parse-asset-type-property-groups
  [xml-content]
  (map parse-property-group (get-elements-of-type xml-content :propertyGroup)))

(defn- parse-asset-type
  [xml]
  (let [{{:keys [name id parentTypeId icon description version]} :attrs
         content :content}
        xml]
    (struct-map database/asset-type
      :id (parse-id id)
      :parent-type-id (parse-id parentTypeId)
      :name name
      :icon icon
      :description description
      :version (parse-number version)
      :property-groups (parse-asset-type-property-groups content))))

(defn- get-version
  [entity]
  (or (:version entity) 0))

(def version-mismatch-exception-message "Version mismatch")

(defn- verify-version
  [db current-entity new-entity]
  (when current-entity
    (when-not (= (get-version current-entity) (get-version new-entity))
      (throw (IllegalArgumentException.
              ^String version-mismatch-exception-message)))))

(defn- increment-version
  [entity]
  (update-in entity [:version] #(inc (or % 0))))

(defn filter-assets-with-any-given-property
  [db property-keys]
  (let [property-key-set (set property-keys)]
    (filter
      (fn [asset]
        (some #(get-asset-property asset %) property-key-set))
      (database/get-entities db :assets))))

(defn delete-values-of-properties
  [db property-keys]
  (let [property-keys-set (set property-keys)]
    (reduce
      (fn [db asset]
        (database/put-entity!
          db
          :assets
          (update-in asset [:properties]
                     (fn [properties]
                       (remove
                        (fn [[k v]] (contains? property-keys-set k))
                        properties)))))
      db
      (filter-assets-with-any-given-property db property-keys))))

(defn removed-property-keys
  [old-asset-type new-asset-type]
  (let [get-property-path-set
        (fn [asset-type]
          (->> [asset-type]
               (get-asset-type-property-paths-from-asset-types)
               (map key-from-property-path)
               (set)))]
    (vec
     (apply clojure.set/difference
            (map get-property-path-set [old-asset-type new-asset-type])))))


(defn update-asset-type
  [db id asset-type]
  (let [current-asset-type (database/get-entity-by-id db :asset-types id)
        updated-asset-type (-> asset-type
                             (assoc :id id)
                             (update-in [:property-groups]
                                        assign-property-group-ids)
                             (increment-version))]
    (verify-version db current-asset-type asset-type)
    (-> db
      (database/put-entity!
        :asset-types
        updated-asset-type)
      (delete-values-of-properties
       (removed-property-keys current-asset-type updated-asset-type)))))

;; Search (asset type counts) ;;

(defn- get-asset-types-with-asset-counts
  [db predicate]
  (let [asset-types (database/get-entities db :asset-types)
        assets (->> (database/get-entities db :assets)
                    (set)
                    (select predicate))
        report (grouping/get-asset-types db assets asset-types)]
    (into [:assetTypes]
          (->> (grouping/get-asset-types db assets asset-types)
               (map
                (fn [{asset-type :attribute assets :values}]
                  [:assetType
                   (-> (get-attributes-from-entity asset-type [:id :name :icon])
                       (assoc :assetCount (count assets)))]))))))

(defn- parse-get-asset-types-request-asset-type-predicate
  [db request]
  (if-let [map-id (-> request :params :map parse-id)]
    (create-asset-on-map-predicate db map-id)
    (constantly true)))

(defn- get-asset-types
  [db request]
  (if (-> request :params :show (= "assetCounts"))
    (let [pred (parse-get-asset-types-request-asset-type-predicate db request)]
      (to-xml-str
        (get-asset-types-with-asset-counts db pred)))
    (read-entities db :asset-type
                   [:id :name :icon :parent-type-id :version] :name)))

(defn create-assets-of-type-element
  [asset-type property-groups assets]
  [:assets
   (asset-type-xml asset-type property-groups)
   (create-asset-elements
     assets
     (fn [id-path]
       (get-type-of-asset-property-from-property-groups property-groups
                                                        id-path)))])

(defn- get-assets-of-type
  [db asset-type property-groups spec-id]
  (filter
    (if-let [asset-spec (database/get-entity-by-id db :asset-specs spec-id)]
      (create-asset-predicate db asset-spec)
      (constantly true))
    (get-assets-of-type-and-descendants db (:id asset-type))))

(defn- assets-of-type-xml
  [db asset-type-id spec-id]
  (when-let [asset-type
             (database/get-entity-by-id db :asset-types asset-type-id)]
    (let [property-groups (get-asset-type-property-groups db asset-type-id)]
      (create-assets-of-type-element
        asset-type
        property-groups
        (get-assets-of-type db asset-type property-groups spec-id)))))

(defn get-asset-type-assets-element
  [db request]
  (when-let [id (parse-request-route-id request)]
    (let [spec-id (-> request :params :assetSpec parse-id)]
      (assets-of-type-xml db id spec-id))))

(defn- get-asset-type-assets
  [db request]
  (to-xml-str (get-asset-type-assets-element db request)))

(defn- do-get-asset-type-card
  [db request]
  (or
    (when-let [id (parse-request-route-id request)]
      (when-let [card (get-asset-type-card db id)]
        (to-xml-str (card->xml card))))
    (create-page-not-found-response)))

(defn- put-asset-type-card!
  [db request]
  (when-let [id (parse-request-route-id request)]
    (let [card (-> (:body request)
                 (parse)
                 (xml->card))]
      (dosync
        (let [asset-type (database/get-entity-by-id db :asset-types id)]
          (database/put-entity! db :asset-types
                                (assoc asset-type :card-desc card)))))))

(defn- create-asset-on-map-spec-by-map-id-request-parameter
  [db request]
  (when-let [map-id-param (-> request :params :mapId)]
    (create-asset-on-map-predicate
      db
      (if (= map-id-param "none")
        nil
        (parse-id map-id-param)))))

(defn- create-asset-in-building-spec-by-building-id-request-parameters
  [db request]
  (when-let [building-id (-> request :params :buildingId parse-id)]
    (grouping/asset-in-building? db building-id)))

(defn- create-asset-of-type-predicate
  [db request]
  (when-let [asset-type-id (-> request :params :assetTypeId parse-id)]
    (create-is-descendant-of-type-asset-predicate db asset-type-id)))

(defn- create-asset-type-value-statistics-of-property-asset-predicates
  [db request]
  (remove nil?
    (map
      #(% db request)
      [create-asset-on-map-spec-by-map-id-request-parameter
       create-asset-in-building-spec-by-building-id-request-parameters
       create-asset-of-type-predicate])))

(defn create-asset-type-property-values-root-element-xml
  [[asset-type-id property-group-id property-id] label type]
  [:assetTypePropertyValues {:assetTypeId asset-type-id
                             :propertyGroupId property-group-id
                             :propertyId property-id
                             :label label
                             :type (database/db-type-to-name type)}])

(defn create-asset-type-property-values-entry-xml
  [value value-count]
  [:entry
   (if-not (nil? value)
     {:value value :count value-count}
     {:count value-count})])

(defn create-asset-type-property-values-xml
  [property-id-path label type key-value-pairs]
  (into
   (create-asset-type-property-values-root-element-xml
    property-id-path label type)
   (map
    (fn [[k v]]
      (create-asset-type-property-values-entry-xml
       (if-not (nil? k) (format-property k type))
       v))
    (sort-by first key-value-pairs))))

(defn create-asset-predicate-from-request-parameters
  [db request]
  (apply every-pred?
         (create-asset-type-value-statistics-of-property-asset-predicates
          db request)))

(defn property-frequencies
  [asset-types assets property-id-path]
  (frequencies
   (get-asset-type-property-values asset-types assets property-id-path)))

(defn- get-asset-type-value-statistics-of-property
  [db property-id-path request]
  (when (not-any? nil? property-id-path)
    (let [asset-types (database/get-entities db :asset-types)]
      (if-let [asset-type-property
               (find-asset-type-property asset-types
                                         property-id-path)]
        (to-xml-str
         (create-asset-type-property-values-xml
          property-id-path
          (:label asset-type-property)
          (:type asset-type-property)
          (property-frequencies asset-types
                                (filter
                                 (create-asset-predicate-from-request-parameters db request)
                                 (database/get-entities db :assets))
                                property-id-path)))
        (create-page-not-found-response)))))

(defn do-get-asset-type-value-statistics
  [db id]
  (when id
    (when-let [statistics (get-asset-type-value-satistics db id)]
      (into [:assetTypeValueStatistics]
            (map
             (fn [asset-type-statistics]
               (into
                [:assetType (get-xml-attributes
                             (:asset-type asset-type-statistics)
                             [:id :name])]
                (map
                 (fn [property-group-statistics]
                   (into
                    [:propertyGroup (get-xml-attributes
                                     (:property-group property-group-statistics)
                                     [:id :name])]
                    (map
                     (fn [property-statistics]
                       (into
                        [:property (get-xml-attributes
                                    (:property property-statistics)
                                    [:id :label :type]
                                    {:type asset-property-type-to-xml})]
                        (map
                         (fn [[v value-count]]
                           [:entry (hash-map-with-non-nil-values
                                     :value v :count value-count)])
                         (:values property-statistics))))
                     (:properties property-group-statistics))))
                 (:property-groups asset-type-statistics))))
             (:asset-types statistics))))))

(defn- get-asset-type-value-statistics
  [db request]
  (or (to-xml-str (do-get-asset-type-value-statistics
                   db (parse-request-route-id request)))
    (create-page-not-found-response)))

(defn- asset-type-xml-2
  [asset-type db]
  (asset-type-xml asset-type
                  (get-asset-type-property-groups db (:id asset-type))))

(defn- get-asset-type
  [db request]
  (when-let [asset-type (get-database-entity-by-route-id db
                                                         :asset-types
                                                         request)]
    (to-xml-str (asset-type-xml-2 asset-type db))))

(defn- parse-new-asset-type
  [xml db]
  (fill-asset-type-ids db (parse-asset-type xml)))

(defn- dispatch-associate-engine-asset-group
  [db asset-type]
  (send-off
   (agent db)
   (bound-fn [db]
     (try
       (let [asset-type
             (assoc-new-empty-asset-group-to-asset-type! asset-type)]
         (dosync
          (database/put-entity! db :asset-types asset-type)))
       (catch Throwable e
         (error "Error creating engine asset group for asset type" e)))
     db)))

(defn- get-asset-types-req
  [db request]
  ; TODO Make this more like update-asset-type!
  (let [body (parse (:body request))
        new-asset-type (create-database-entity-from-xml! db
                                                         body
                                                         :asset-types
                                                         parse-new-asset-type)]
    (dispatch-associate-engine-asset-group db new-asset-type)
    (to-xml-str (asset-type-xml-2 new-asset-type db))))

(defn- update-asset-type!
  [db request body id]
  (try
    (let [db (dosync (update-asset-type db id (parse-asset-type body)))
          updated-asset-type (database/get-entity-by-id db :asset-types id)]
      (to-xml-str
        (asset-type-xml-2 updated-asset-type db)))
    (catch IllegalArgumentException e
      (when-not (= (.getMessage e) version-mismatch-exception-message)
        (throw e))
      {:status 409 :body "<error type=\"version mismatch\"/>"})))

(defn- delete-entities-by-ids
  [db entity-kw ids]
  (reduce
    #(database/delete-entity! %1 entity-kw %2)
    db
    ids))

(defn delete-asset-type
  [db id]
  (let [asset-types (get-asset-type-and-descendants db id)
        asset-type-ids (set (map :id asset-types))
        assets (select (value-by-key-in-set-pred? :asset-type-id asset-type-ids)
                 (set (database/get-entities db :assets)))]
    (-> db
      (delete-entities-by-ids :asset-types asset-type-ids)
      (delete-entities-by-ids :assets (map :id assets)))))

(defn- dispatch-delete-asset-type-engine-asset-group!
  [asset-type]
  (send-off (agent nil)
    (bound-fn [_]
      (try
        (delete-asset-group (:engine-asset-group-id asset-type))
        (catch Throwable e
          (error "Error removing engine asset group of asset type" e)))
      _)))

(defn- delete-asset-type!
  [db id]
  (do
    (let [asset-type (database/get-entity-by-id db :asset-types id)]
      (when asset-type
        (dispatch-delete-asset-type-engine-asset-group! asset-type))
      (dosync (delete-asset-type db id)))
    (to-xml-str [:deleted])))

(defn get-asset-type-and-ancestors-in-json
  [db asset-type-id]
  (to-json
   (map
    (create-asset-type-to-json-fn db)
    (get-asset-type-and-ancestors db asset-type-id))))

(defn get-asset-type-icon
  [db asset-type-id]
  (when-let [asset-type (database/get-entity-by-id
                       db :asset-types asset-type-id)]
    (if-let [icon (:icon asset-type)]
      icon
      (when-let [parent-id (:parent-type-id asset-type)]
        (get-asset-type-icon db parent-id)))))

(defn parse-id-path
  [id-path]
  (vec (map parse-id id-path)))

(defn predicate-from-json-asset-spec
  [db json-asset-spec]
  (when json-asset-spec
    (create-asset-predicate
     db
     (json-asset-spec/parse-json-asset-spec json-asset-spec))))

(defn property-value-statistics
  [db json-request]
  (let [property-path (parse-id-path (:idPath json-request))
        asset-types (database/get-entities db :asset-types)
        property (find-asset-type-property asset-types
                                           property-path)]
    (when property
      (let [type (:type property)
            request-property-frequencies #(property-frequencies asset-types % property-path)
            assets (let [assets (database/get-entities db :assets)]
                     (if-let [pred (predicate-from-json-asset-spec db (:assetSpec json-request))]
                       (filter pred assets)
                       assets))
            total-property-frequencies (request-property-frequencies assets)
            secondary-property-frequencies (when-let [secondary-pred (predicate-from-json-asset-spec db (:secondarySpec json-request))]
                                             (request-property-frequencies 
                                              (filter secondary-pred assets)))
            value-counts (if-not secondary-property-frequencies
                           (juxt total-property-frequencies)
                           (juxt #(or (secondary-property-frequencies %) 0)
                                 total-property-frequencies))]
        (to-json
         (map
          (juxt #(format-property % type) value-counts)
          (keys total-property-frequencies)))))))

(defn create-get-asset-type-lineage-of-asset
  [asset-types]
  (let [ancestor-asset-types (create-ancestor-asset-types-map asset-types)]
    (fn [{id :asset-type-id}]
      (cons id (get ancestor-asset-types id)))))

(defn get-secondary-frequencies
  [assets secondary-pred get-asset-type-lineage-of-asset]
  (when secondary-pred
    (frequencies
     (mapcat get-asset-type-lineage-of-asset (filter secondary-pred assets)))))

(defn asset-count-per-type
  [db body]
  (let [primary-pred (predicate-from-json-asset-spec db (:assetSpec body))
        secondary-pred (predicate-from-json-asset-spec db (:secondarySpec body)) 
        assets (filter primary-pred (database/get-entities db :assets))
        get-asset-type-lineage-of-asset (create-get-asset-type-lineage-of-asset
                                         (database/get-entities db :asset-types))
        secondary-frequencies (get-secondary-frequencies assets
                                                         secondary-pred
                                                         get-asset-type-lineage-of-asset)
        total-frequencies (frequencies
                           (mapcat get-asset-type-lineage-of-asset assets))]
    (to-json {:assetTypes (if secondary-frequencies
                            (map (fn [[k v]]
                                   [k [(get secondary-frequencies k) v]])
                                 total-frequencies)
                            (map (fn [[k v]] [k [v]])
                                 total-frequencies))
              :total (count assets)})))

(defn create-asset-type-routes
  [db]
  (routes
   (->
    (routes
     (GET "/assetTypes.json" {:as request}
          (to-json
           (map
            (create-asset-type-to-json-fn db)
            (database/get-entities db :asset-types))))
     (GET "/assetTypes/:id/withAncestors.json" {:as request}
          (or (get-asset-type-and-ancestors-in-json
               db (parse-request-route-id request))
              (create-json-resource-not-found-response request)))
     (GET "/assetTypes/:id.json" {:as request}
          (if-let [asset-type (get-database-entity-by-route-id
                               db :asset-types request)]
            (to-json ((create-asset-type-to-json-fn db) asset-type))
            (create-json-resource-not-found-response request)))
     (POST "/assetTypes/propertyValueStatistics.json" {:keys [body] :as request}
           (or
            (property-value-statistics db (json/read-json (io/reader body)))
            (create-json-resource-not-found-response request)))
     (POST "/assetTypes/assetCountPerType.json" {:keys [body] :as request}
           (asset-count-per-type db (json/read-json (io/reader body)))))
    (with-json-content-type))
   (->
    (routes
     (GET "/assetTypes" {:as request} (get-asset-types db request))

     (GET "/assetTypes/:id/assets" {:as request} (get-asset-type-assets db request))

     (GET "/assetTypes/:id/card" {:as request} (do-get-asset-type-card db request))

     (PUT "/assetTypes/:id/card" {:as request} (put-asset-type-card! db request))

     (GET "/assetTypes/:id/valueStatistics/:property-group-id/:property-id"
          {{id :id
            property-group-id :property-group-id
            property-id :property-id} :params
            :as request}
          (get-asset-type-value-statistics-of-property
           db
           (parse-id-path [id property-group-id property-id])
           request))

     (GET "/assetTypes/:id/valueStatistics" {:as request}
          (get-asset-type-value-statistics db request))

     (GET "/assetTypes/:id.png" {:as request}
          (redirect (str "/icons/"
                         (get-asset-type-icon db
                                              (parse-request-route-id
                                               request)))))

     (GET "/assetTypes/:id" {:as request} (get-asset-type db request))

     (POST "/assetTypes" {:as request} (get-asset-types-req db request))

     (PUT "/assetTypes/:id" {:as request}
          (let [id (parse-request-route-id request)
                body (parse (:body request))]
            (update-asset-type! db request body id)))

     (DELETE "/assetTypes/:id" {:as request}
             (let [id (parse-request-route-id request)]
               (delete-asset-type! db id))))

    (with-xml-content-type))))
