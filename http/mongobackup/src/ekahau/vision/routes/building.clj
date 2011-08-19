(ns ekahau.vision.routes.building
  (:require
   [clojure.contrib.io :as io]
   [clojure.contrib.string :as str]
   [ekahau.vision
    [columns :as columns]
    [database :as database]
    [grouping :as grouping]
    [uri :as uri]]
   [ekahau.vision.model
    [asset :as asset]
    [building :as building]
    [positions :as positions]
    [site :as site]]
   [ekahau.time :as time]
   [ekahau.vision.site-service :as site-service])
  (:use
   [clojure.contrib.json :only [read-json]]
   compojure.core
   clojure.set
   [clojure.contrib.str-utils :only [str-join]]
   clojure.contrib.pprint
   [clojure.contrib.trace :only [trace]]
   [clj-time.core :only [hours]]
   [ekahau.predicates :only [every-pred?]]
   [ekahau.vision.model.asset :only
    [create-asset-instance-of-any-given-type]]
   [ekahau.vision.predicate :only
    [create-asset-predicate
     get-asset-predicate-by-spec-id]]
   [ekahau.string :only [parse-id parse-number]]
   [ekahau.seq-utils :only [count-per-predicate]]
   [ekahau.vision.site-service :only [create-get-asset-count-cell-value-fn]]
   [ekahau.vision.xml.map :only [map-xml]]
   [ekahau.vision.xml.asset-type :only [asset-types-xml]]
   [ekahau.vision.xml.report :only
    [create-data-key-string create-report-xml-structure]]
   [ekahau.vision.routes.helpers :only
    [get-user-by-request-session
     parse-request-route-id
     to-json
     with-xml-content-type
     with-json-content-type
     create-page-not-found-response]]
   [ekahau.xml :only [to-xml-str]]
   [ekahau.util :only [keys-recursively-to-camel-case]]))

(defn get-site-view-rows
  [db request]
  (:site-view-rows (get-user-by-request-session db request)))

(defn- assoc-sub-group
  [db create-sub-group entry]
  (assoc entry :sub-group (create-sub-group db (:values entry))))

(defn create-building-report
  [db building-report create-sub-group]
  (assoc-sub-group db create-sub-group building-report))

(defn- create-floor-reports
  [db building building-report create-sub-group]
  (->> (:values building-report)
       (grouping/get-building-floors db building)
       (map
        (partial assoc-sub-group db create-sub-group))))

(defn- create-building-floors-report
  [db building assets create-sub-group]
  (when-let [building-report (grouping/get-building db building assets)]
    {:building (create-building-report db
                                       building-report
                                       create-sub-group)
     :floors (create-floor-reports db
                                   building
                                   building-report
                                   create-sub-group)}))

(defn create-create-asset-types-sub-group-fn
  [asset-types]
  (fn [db assets]
    (grouping/get-asset-types db assets asset-types)))

(defn- create-asset-count-str
  [assets pred]
  (if pred
    (let [matching-count (count (filter pred assets))]
      (if (< 0 matching-count)
        (str matching-count "/" (count assets))
        (str (count assets))))
    (str (count assets))))

(defn- create-asset-types-report-xml
  [report asset-pred]
  [:assetTypeReport
   (->> report
     (map #(-> % :values (create-asset-count-str asset-pred)))
     (interpose ",")
     (apply str))])

(defn- create-floor-report-elements
  [floors create-sub-group-report-xml]
  (map
    (fn [entry]
      (let [m (-> entry :attribute :map)]
        [:floor {:mapId (:id m) :name (:name m)}
         (create-sub-group-report-xml (:sub-group entry))]))
    floors))

(defn- create-building-floors-report-xml
  [{:keys [building floors]} create-sub-group-report-xml & header-elements]
  (into
    [:building (:attribute building)
     header-elements
     (create-sub-group-report-xml (:sub-group building))]
    (create-floor-report-elements floors create-sub-group-report-xml)))

(defn- create-building-floors-report-from-db
  [db id asset-types]
  (create-building-floors-report
   db
   (database/get-entity-by-id db :buildings id)
   (database/get-entities db :assets)
   (create-create-asset-types-sub-group-fn asset-types)))

(defn- create-asset-type-subgroup-report-xml-fn
  [db asset-spec-id]
  (let [asset-pred (get-asset-predicate-by-spec-id db asset-spec-id)]
    (fn [report] (create-asset-types-report-xml report asset-pred))))

(defn- create-building-response
  [db id asset-spec-id]
  (or (let [asset-types (asset/preorder-asset-type-traversal
                         (database/get-entities db :asset-types))]
        (when-let [building-floors (create-building-floors-report-from-db
                                    db id asset-types)]
          (->>
           (create-building-floors-report-xml
            building-floors
            (create-asset-type-subgroup-report-xml-fn db asset-spec-id)
            (asset-types-xml asset-types nil))
           (to-xml-str))))
      (create-page-not-found-response)))

(defn- create-buildings-response-buildings-part
  [db create-sub-group-report-xml assets create-sub-group]
  (->>
   (database/get-entities db :buildings)
   (map
    #(create-building-floors-report db % assets create-sub-group))
   (map
    #(create-building-floors-report-xml % create-sub-group-report-xml))))

(defn- create-buildings-response-maps-without-building-part
  [db create-sub-group-report-xml assets create-sub-group]
  (let [all-maps (database/get-entities db :maps)
        map-reports (grouping/get-maps-not-in-any-building db all-maps assets)]
    (when (seq map-reports)
      (list
       (into
        [:maps]
        (map
         (fn [entry]
           [:map (select-keys (:attribute entry) [:id :name])
            (create-sub-group-report-xml
             (create-sub-group db (:values entry)))])
         map-reports))))))

(defn- create-buildings-response-assets-without-position-part
  [db create-sub-group-report-xml assets create-sub-group]
  (let [assets-without-position (positions/get-assets-without-position db assets)]
   (when (seq assets-without-position)
     (list
      [:unknownPosition {:name "Unknown Location" :type "none"}
       (create-sub-group-report-xml
        (create-sub-group db assets-without-position))]))))

(defn create-create-subgroup-report-fn
  [db asset-types asset-spec-id]
  (let [create-sub-group
        (create-create-asset-types-sub-group-fn asset-types)

        create-asset-type-subgroup-report-xml
        (create-asset-type-subgroup-report-xml-fn db asset-spec-id)]
    (fn [assets]
      (create-asset-type-subgroup-report-xml (create-sub-group db assets)))))




(defn create-site-report-building-part
  [db building assets create-sub-group]
  (when-let [building-report (grouping/get-building db building assets)]
    {:name (:name building)
     :values (->> (create-building-report db
                                          building-report
                                          create-sub-group)
                  :sub-group
                  (map
                   #(or (:values %) #{})))
     :children []}))

(defn create-site-report-buildings
  [db assets create-sub-group]
  (->> (database/get-entities db :buildings)
       (sort-by :name)
       (map #(create-site-report-building-part db % assets create-sub-group))))

(defn create-site-report
  [db asset-spec-id]
  (let [asset-types (asset/preorder-asset-type-traversal
                     (database/get-entities db :asset-types))
        assets (database/get-entities db :assets)
        create-sub-group (create-create-asset-types-sub-group-fn asset-types)]
    {:name "All"
     :values (map :values (create-sub-group db assets))
     :children (create-site-report-buildings db assets create-sub-group)}))

(defn- create-buildings-response
  [db asset-spec-id]
  (let [asset-types (asset/preorder-asset-type-traversal
                     (database/get-entities db :asset-types))
        assets (database/get-entities db :assets)
        create-sub-group (create-create-asset-types-sub-group-fn asset-types)

        create-sub-group-report-xml
        (create-asset-type-subgroup-report-xml-fn db asset-spec-id)

        create-subgroup-report
        (create-create-subgroup-report-fn db asset-types asset-spec-id)

        report (create-site-report db asset-spec-id)]
    (to-xml-str
      (into
        [:buildings {:name (:name report)}]
        (concat
          (list
           (asset-types-xml asset-types nil))
          (list
           (create-subgroup-report assets))
          (create-buildings-response-buildings-part
           db
           create-sub-group-report-xml
           assets
           create-sub-group)
          (create-buildings-response-maps-without-building-part
           db
           create-sub-group-report-xml
           assets
           create-sub-group)
          (create-buildings-response-assets-without-position-part
           db
           create-sub-group-report-xml
           assets
           create-sub-group))))))

(defn- get-zones-by-map-id
  [db]
  (reduce (fn [m zone]
            (let [map-id (-> zone :area :map-id)]
              (assoc m map-id (conj (get m map-id []) zone))))
    {} (database/get-entities db :zones)))

(defn- create-structure-response
  [db]
  (let [zones-by-map-id (get-zones-by-map-id db)]
    (to-xml-str
      (into
        [:buildings]
        (map
          (fn [building]
            (into [:building {:id (:id building) :name (:name building)}]
              (map
                (fn [{m :map}]
                  (into [:map {:id (:id m) :name (:name m)}]
                    (map (fn [zone] [:zone {:id (:id zone) :name (:name zone)}])
                      (sort-by :name (get zones-by-map-id (:id m))))))
                (grouping/get-sorted-building-floor-map-entries db (:id building)))))
          (sort-by :name (database/get-entities db :buildings)))))))

;; Group assets by Site > Building > Floor ;;

(defn create-map-fields
  [{:keys [id name]}]
  {:name name
   :uri (uri/map-uri id)
   :key {:map id}})

(defn get-zones-of-map
  [db map-id]
  (filter
   #(= map-id (get-in % [:area :map-id]))
   (database/get-entities db :zones)))

(defn create-zone-fields
  [{:keys [id name]}]
  {:name name
   :uri (uri/zone-uri id)
   :key {:zone id}})

(defn assoc-zones-as-children
  [map-fields db map-id]
  (if-let [zones (seq (get-zones-of-map db map-id))]
    (assoc map-fields
      :children
      (map create-zone-fields (sort-by :name zones)))
    map-fields))

(defn create-map-of-building
  [db {:keys [id name] :as a-map}]
  (assoc-zones-as-children
   (create-map-fields a-map)
   db id))

(defn create-floor-of-building
  [db {:keys [map-id] :as floor}]
  (create-map-of-building db (database/get-entity-by-id db :maps map-id)))

(defn create-maps-in-building
  [db building-id]
  (vec
   (map
    (partial create-floor-of-building db)
    (ekahau.vision.model.building/get-sorted-floors-of-building
     db building-id))))

(defn create-building-fields
  [{:keys [id name]}]
  {:name name
   :uri (uri/building-uri id)
   :key {:building id}})

(defn create-building-of-site
  [db {:keys [id] :as building}]
  (merge
   (create-building-fields building)
   {:children (create-maps-in-building db id)}))

(defn create-maps-in-no-building
  [db maps]
  (vec
   (sort-by :name
    (map
     (partial create-map-of-building db)
     maps))))

(defn create-no-building-fields
  []
  {:name "No Building"
   :uri "buildings/none"
   :key {:building :none}})

(defn create-no-building
  [db]
  (let [maps-without-building (building/get-maps-not-in-any-building db)]
    (when (seq maps-without-building)
      [(merge
        (create-no-building-fields)
        {:children (create-maps-in-no-building db maps-without-building)})])))

(defn create-unknown
  [db]
  [{:name "Unknown"
    :uri "maps/unknown"
    :key {:map nil}}])

(defn create-existing-buildings
  [db]
  (map
   (partial create-building-of-site db)
   (sort-by :name (database/get-entities db :buildings))))

(defn create-buildings-in-site
  [db]
  (vec
   (concat
    (create-existing-buildings db)
    (create-no-building db)
    (create-unknown db))))

(defn create-zone-group-of-grouping
  [grouping-id {:keys [id name]}]
  {:name name
   :uri (uri/zone-group-uri grouping-id id)
   :key {:zone-grouping grouping-id
         :zone-group id}})

(defn create-zone-groups-of-grouping
  [{:keys [id groups]}]
  (map
   (partial create-zone-group-of-grouping
            id)
   groups))

(defn create-zone-grouping
  [db zone-grouping]
  {:name (:name zone-grouping)
   :uri (uri/zone-grouping-uri (:id zone-grouping))
   :children (create-zone-groups-of-grouping zone-grouping)
   :key {:zone-grouping (:id zone-grouping)}})

(defn create-zone-groupings-in-site
  [db]
  [{:name "Zone Groupings"
    :uri "zoneGroupings"
    :children
    (map
     (partial create-zone-grouping db)
     (database/get-entities db :zone-groupings))}])

(defn create-site-hierarchy
  [db]
  {:name "All"
   :uri "buildings"
   :children (concat
              (create-buildings-in-site db)
              (create-zone-groupings-in-site db))
   :key {:buildings :all}})

;; Asset Type Grouping ;;

(defn add-asset-to-entities-of-asset-type
  [asset m asset-type]
  (update-in m [{:uri (uri/asset-type-uri (:id asset-type))} :entities]
             (fnil conj #{}) asset))

(defn add-asset-to-entities-of-asset-type-and-ancestors
  [db result asset]
  (reduce
   (partial add-asset-to-entities-of-asset-type asset)
   result
   (asset/get-all-asset-types db (:asset-type-id asset))))

;; Asset Specification Grouping ;;

(defn group-by-specifications
  [db assets spec-pred-pairs]
  (into
   {}
   (map
    (fn [[spec pred]]
      [{:uri (uri/asset-spec-uri (:id spec))}
       {:entities (set (filter pred assets))}])
    spec-pred-pairs)))

;; Asset Type Grouping on Site Hierarchy ;;

(defn site-nodes-in-preorder
  [node]
  (cons node (mapcat #(site-nodes-in-preorder %) (:children node))))

(defn uri-and-entities
  [node]
  (select-keys node [:uri :entities]))

;; Get Buildings - in JSON ;;

(defn filter-site-hierarchy
  "Returns a hierarchy of nodes for which (pred node) returns true.
  No children of a parent for which (pred node) return false are included."
  [pred node]
  (if (:children node)
    (assoc node
      :children (->> (:children node)
                     (filter pred)
                     (map #(filter-site-hierarchy pred %))))
    node))

(defn rows-from-site-hierarchy
  [site-hierarchy]
  (let [update-all-nodes
        (fn this [node]
          (select-keys
           (let [{:keys [children]} node]
             (if (seq children)
               (assoc node
                 :children
                 (map this children))
               node))
           [:name :uri :children]))]
    (update-all-nodes site-hierarchy)))

(defn get-column-keys
  [{asset-type-ids :asset-types
    asset-spec-ids :asset-specs
    include-all? :include-all?
    include-total? :include-total?
    :or {include-all? true
         include-total? true}}]
  (concat
   (when include-all? [{:asset-spec :all}])
   (map (fn [id] {:asset-type id}) asset-type-ids)
   (map (fn [id] {:asset-spec id}) asset-spec-ids)
   (when include-total? [{:asset-spec :total}])))

(defn get-graphed-row-key
  [row-key]
  (let [ks #{:map :zone :zone-group}]
    (some ks (keys row-key))))

(defn get-row-total-found-value
  [get-cell-value row-key]
  (first (get-cell-value [row-key {:asset-spec :total}])))

(defn create-buildings-values
  [row-keys column-keys get-cell-value]
  (map
   (fn [row-key]
     (concat
      (map
       (fn [column-key]
         (get-cell-value [row-key column-key]))
       column-keys)))
   row-keys))

(defn node-pred-from-row-selection
  [row-selection]
  (or (and row-selection
           (let [row-uri-set (set row-selection)]
             (fn [node]
               (row-uri-set (:uri node)))))
      (constantly true)))

(defn ordered-row-keys-from-site-hierarhcy
  "Returns a sequence of row keys.

  The sequence of nodes is in preorder traversal order of site-hierarchy."
  [site-hierarchy]
  (map :key (site-nodes-in-preorder site-hierarchy)))

(defn get-buildings
  [db specification]
  (let [{:keys [rows columns asset-spec-id]} specification]
    (dosync
     (let [get-cell-value (create-get-asset-count-cell-value-fn db
                                                                columns
                                                                asset-spec-id)
           site-hierarchy (filter-site-hierarchy
                           (every-pred?
                            (node-pred-from-row-selection rows)
                            (fn [node]
                              (or
                               (nil? (:key node))
                               (some #{:zone-grouping} (keys (:key node)))
                               (if asset-spec-id
                                 (if-let [value (get-cell-value
                                                 [(:key node)
                                                  {:asset-spec :total}])]
                                   (or (and (< 1 (count value))
                                            (< 0 (first value)))))
                                 true))))
                           (create-site-hierarchy db))
           row-keys (ordered-row-keys-from-site-hierarhcy site-hierarchy)]
       {:rows (rows-from-site-hierarchy site-hierarchy)
        :columns (columns/create-buildings-columns db columns)
        :values (create-buildings-values
                 row-keys
                 (get-column-keys columns)
                 get-cell-value)}))))

(defn get-buildings-in-json
  [db specification]
  (to-json
   (get-buildings db specification)))

(defn get-site-view-buildings-in-json
  [db specification]
  (get-buildings-in-json db specification ))

(defn create-map-of-map-id
  [site map-id]
  (let [zones (site/get-zones-of-map site map-id)]
    (assoc (create-map-fields (site/get-map-by-id site map-id))
      :zones (map (fn [{:keys [id name]}]
                    {:uri (uri/zone-uri id)
                     :name name})
                  zones))))

(defn create-map-of-floor
  [site floor]
  (create-map-of-map-id site (:map-id floor)))

(defn create-maps-of-building
  [site building]
  (map
   (partial create-map-of-floor site)
   (site/get-floors-of-building site (:id building))))

(defn create-buildings-structure-building
  [site building]
  (merge
   (create-building-fields building)
   {:maps
    (create-maps-of-building site building)}))

(defn create-buildings-structure-buildings
  [site]
  (map
   (partial create-buildings-structure-building site)
   (site/get-buildings site)))

(defn create-buildings-structure-buildings-for-maps-without-building
  [site]
  (when-let [maps-without-building
             (seq (site/get-maps-in-no-building site))]
    [(merge
      (create-no-building-fields)
      {:maps
       (map
        (partial create-map-of-map-id site)
        (map :id maps-without-building))})]))

(defn get-buildings-structure
  [db]
  (let [site (site/new-database-site db)]
   (concat
    (create-buildings-structure-buildings site)
    (create-buildings-structure-buildings-for-maps-without-building site))))

(defn get-buildings-structure-in-json
  [db]
  (to-json (get-buildings-structure db)))

(defn get-buildings-maps-and-zones-structure-in-json
  [db]
  (to-json (rows-from-site-hierarchy (create-site-hierarchy db))))

(defn parse-asset-spec-id
  [request]
  (parse-id (-> request :params :assetSpec)))

;; User based column configuration ;;

(defn get-site-view-columns
  [db request]
  (or (:site-view-columns (get-user-by-request-session db request))
      (columns/get-default-site-view-columns db)))

(defn site-view-columns-to-json
  [{:keys [asset-types asset-specs]}]
  (to-json {:assetTypes asset-types
            :assetSpecs asset-specs}))

(defn get-my-site-view-columns-json
  [db request]
  (site-view-columns-to-json (get-site-view-columns db request)))

(defn put-my-site-view-columns-json!
  [db request]
  (let [content (read-json (io/reader (:body request)))]
    (dosync
     (let [user (get-user-by-request-session db request)]
       (database/put-entity!
        db :users
        (assoc user
          :site-view-columns {:asset-types (->> (:assetTypes content)
                                                (vec))
                              :asset-specs (->> (:assetSpecs content)
                                                (vec))})))))
  (to-json {:result "OK"}))

(defn put-my-site-view-columns-asset-specs-json!
  [db request]
  (let [content (read-json (io/reader (:body request)))]
    (dosync
     (let [user (get-user-by-request-session db request)]
       (database/put-entity!
        db :users
        (update-in user [:site-view-columns :asset-specs]
                   (fn [specs]
                     (if-not (some #(= content %) specs)
                       (conj specs content)
                       specs)))))))
  (to-json {:result "OK"}))

(defn get-my-site-view-rows-json
  [db request]
  (to-json (get-site-view-rows db request)))

(defn put-my-site-view-rows-json!
  [db request]
  (let [content (read-json (io/reader (:body request)))]
    (let [user (get-user-by-request-session db request)]
      (database/put-entity!
       db :users
       (assoc user
         :site-view-rows content))))
  (to-json {:result "OK"}))

(defn site-view-specification-from-request
  [db request]
  {:asset-spec-id (parse-asset-spec-id request)
   :rows (get-site-view-rows db request)
   :columns (get-site-view-columns db request)})

(defn get-my-site-view-configuration-json
  [db request]
  (to-json
   (keys-recursively-to-camel-case
    (select-keys (get-user-by-request-session db request)
                 [:site-view-columns :site-view-rows]))))

(defn put-my-site-view-configuration-json
  [db request]
  (let [content (read-json (io/reader (:body request)))
        user (get-user-by-request-session db request)]
    (database/put-entity!
     db :users
     (assoc user
       :site-view-rows (:siteViewRows content)
       :site-view-columns {:asset-types (->> content
                                             :siteViewColumns
                                             :assetTypes
                                             (vec))
                           :asset-specs (->>  content
                                              :siteViewColumns
                                              :assetSpecs
                                              (vec))}))))

;; ## Bookmarks

(defn get-my-bookmarks-json
  [db request]
  (to-json
   (or (:bookmarks (get-user-by-request-session db request))
       [])))

(defn put-my-bookmarks-json
  [db request]
  (let [content (read-json (io/reader (:body request)))
        user (get-user-by-request-session db request)]
    (database/put-entity!
     db :users
     (assoc user :bookmarks content)))
  (to-json {:result "OK"}))

;; Routes ;;

(defn create-building-routes
  ([db]
     (routes
      (->
       (routes
        (GET "/buildings/:id" {:as request}
          (let [asset-spec-id (parse-asset-spec-id request)]
            (create-building-response db
                                      (parse-request-route-id request)
                                      asset-spec-id)))
        (GET "/buildings" {:as request}
          (let [asset-spec-id (parse-asset-spec-id request)]
            (condp = (-> request :params :format)
                "structure" (create-structure-response db)
                (create-buildings-response db asset-spec-id)))))
       (with-xml-content-type))
      (->
       (routes
        (GET "/buildings.json" {:as request}
          (let [format (get-in request [:params :format] "default")]
            (condp = format
                "default" (get-site-view-buildings-in-json
                           db (site-view-specification-from-request db request))
                "structure" (get-buildings-structure-in-json db)
                "totalStructure" (get-buildings-maps-and-zones-structure-in-json
                                  db))))
        (GET "/users/me/siteViewColumns.json" {:as request}
          (get-my-site-view-columns-json db request))
        (PUT "/users/me/siteViewColumns.json" {:as request}
          (put-my-site-view-columns-json! db request))
        (GET "/users/me/siteViewRows.json" {:as request}
          (get-my-site-view-rows-json db request))
        (PUT "/users/me/siteViewRows.json" {:as request}
          (put-my-site-view-rows-json! db request))
        (PUT "/users/me/siteViewColumns/assetSpecs.json" {:as request}
          (put-my-site-view-columns-asset-specs-json! db request))
        (GET "/users/me/siteViewConfiguration.json" {:as request}
          (get-my-site-view-configuration-json db request))
        (PUT "/users/me/siteViewConfiguration.json" {:as request}
          (put-my-site-view-configuration-json db request))
        (GET "/users/me/bookmarks.json" {:as request}
          (get-my-bookmarks-json db request))
        (PUT "/users/me/bookmarks.json" {:as request}
          (put-my-bookmarks-json db request)))
       (with-json-content-type)))))
