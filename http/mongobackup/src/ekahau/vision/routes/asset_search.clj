(ns ekahau.vision.routes.asset-search
  (:require
    [ekahau.vision.database :as database]
    [ekahau.vision.engine-service]
    [clojure.contrib.seq-utils :as seq]
    [clojure.contrib.str-utils :as str]
    [ekahau.vision.xml.asset :as xml.asset])
  (:use
    compojure.core
    [clojure.contrib.trace :only [trace]]
    [ekahau.predicates :only [substring-pred?]]
    [ekahau.string :only [parse-id parse-calendar-day]]
    [ekahau.vision.predicate :only [create-asset-on-map-predicate
                                    create-property-value-predicate-of-type
                                    get-matching-property-value-assets]]
    [ekahau.vision.routes.helpers
     :only [with-xml-content-type
            create-resource-not-found-response
            create-bad-request-response
            parse-id-string]]
    [ekahau.vision.model.asset :only [find-asset-type-property
                                      get-asset-property]]
    [ekahau.util :only [assoc-multi-kv key-from-property-path]]
    [ekahau.xml :only [get-elements-of-type to-xml-str]]))

(defn- update-assets-per-value
  [m value asset]
  (assoc m value (conj (get m value [])  asset)))

(defn- property-key-value-asset-triplets-to-assets-per-value-per-property
  [coll]
  (reduce
    (fn [m [property-key value asset]]
      (assoc m
        property-key (update-assets-per-value (get m property-key {}) value asset)))
    {} coll))

(defn- get-assets-per-value-per-property
  [db text calendar-day]
  (property-key-value-asset-triplets-to-assets-per-value-per-property
    (get-matching-property-value-assets db text calendar-day)))

;; create-search-report and helpers ;;

(defn- create-property-element-value-entries
  [assets-per-value asset-predicate property-type]
  (map
    (fn [[value assets]]
      [:entry (into
                {:value (xml.asset/format-property value property-type)
                 :count (count assets)}
                (when asset-predicate
                  [[:filteredCount (count (filter asset-predicate assets))]]))])
    (take 5 (sort-by (fn [[value _]] value) assets-per-value))))

(defn- create-property-element-multiple-values-entry
  [assets-per-value asset-predicate property-type]
  [[:entry (into {:multipleValues true
                  :count (reduce + (map (fn [[_ assets]] (count assets)) assets-per-value))}
             (when asset-predicate
               [[:filteredCount (reduce + (map (fn [[_a assets]] (count (filter asset-predicate assets))) assets-per-value))]]))]])

(defn- create-property-element
  [label property-key property-type assets-per-value asset-predicate]
  (into
    [:property {:id (str/str-join "," property-key)
                :name label
                :type (and property-type (name property-type))
                :totalCount (count assets-per-value)}]
    (create-property-element-value-entries assets-per-value asset-predicate property-type)))

(defn- find-by-id
  [id entities]
  (first (filter #(= id (:id %)) entities)))

(defn- create-property-group-element
  [property-group assets-per-value-per-property asset-predicate]
  (into
    [:propertyGroup (select-keys property-group [:id :name])]
    (map
      (fn [[[_ _ property-id :as property-key] assets-per-value]]
        (let [property (->> property-group :properties (find-by-id property-id))]
          (create-property-element (:label property) property-key (:type property) assets-per-value asset-predicate)))
      assets-per-value-per-property)))

(defn- create-tag-properties-property-group-element
  [assets-per-value-per-property asset-predicate]
  (into
    [:propertyGroup {:id "tag" :name "Tag"}]
    (map
      (fn [[[asset-type-id _ property-id :as property-key] assets-per-value]]
        (create-property-element (name property-id) ["tag" "tag" (name property-id)] :text assets-per-value asset-predicate))
      assets-per-value-per-property)))

(defn- hash-map-group-by
  "This function was copied from cloure.contrib.seq-utils/group-by and the result
  map was changed from sorted-map to hash-map because we're grouping by keys
  that are not comparable. When migrated to Clojure 1.2 we can use group-by
  from core library. "
  [f coll]
  (reduce
    (fn [ret x]
      (let [k (f x)]
        (assoc ret k (conj (get ret k []) x))))
    (hash-map) coll))

(defn- group-by-nth-field-of-key
  [index-of-key entries]
  (hash-map-group-by #(get (key %) index-of-key) entries))

(defn- get-property-group
  [asset-type id]
  (->> asset-type :property-groups (find-by-id id)))

(defn- create-asset-type-element
  [asset-type assets-per-value-per-property asset-predicate]
  (into
    [:assetType (select-keys asset-type [:id :name])]
    (map
      (fn [[property-group-id assets-per-value-per-property]]
        (if (= :tag property-group-id)
          (create-tag-properties-property-group-element assets-per-value-per-property asset-predicate)
          (let [property-group (get-property-group asset-type property-group-id)]
            (create-property-group-element property-group assets-per-value-per-property asset-predicate))))
      (sort-by
        (let [index-map (into {} (map vector (concat (map :id (:property-groups asset-type)) [:tag]) (iterate inc 0)))]
          (fn [[property-group-id _ _]]
            (index-map property-group-id)))
        (group-by-nth-field-of-key 1 assets-per-value-per-property)))))

(defn- create-asset-predicate
  [db map-id asset-type-id]
  (ekahau.vision.predicate/create-asset-predicate
    db
    {:type "and"
     :specs (remove
              nil?
              (vector
                (when map-id {:type "map" :map-id map-id})
                (when asset-type-id {:type "asset-type" :asset-type-id asset-type-id})))}))

(defn- get-assets-by-engine-asset-id
  [db]
  (->> (database/get-entities db :assets)
    (filter #(-> % :engine-asset-id))
    (map
      (fn [asset]
        [(:engine-asset-id asset) asset]))
    (into {})))

(defn- get-assets-per-value-per-tag-property
  [db text]
  (let [assets-by-engine-asset-id (get-assets-by-engine-asset-id db)]
    (property-key-value-asset-triplets-to-assets-per-value-per-property
      (mapcat
        (fn [[engine-asset-id tag-properties]]
          (when-let [asset (get assets-by-engine-asset-id engine-asset-id)]
            (map
              (fn [[k v]]
                [[:tag :tag k] v asset])
              tag-properties)))
        (ekahau.vision.engine-service/get-matching-tags text)))))

(defn- create-search-report
  [db text map-id calendar-day asset-type-id]
  (let [asset-predicate (create-asset-predicate db map-id asset-type-id)]
    (->> (merge (get-assets-per-value-per-property db text calendar-day)
           (get-assets-per-value-per-tag-property db text))
      (group-by-nth-field-of-key 0)
      (map
        (fn [[asset-type-id assets-per-value-per-property]]
          (let [asset-type (when (not= :tag asset-type-id)
                               (database/get-entity-by-id db :asset-types asset-type-id))]
            (create-asset-type-element asset-type assets-per-value-per-property asset-predicate))))
      (into [:searchResult {:text text}]))))

(defn- search-stats
  [db request]
  (let [{:keys [text mapId calendarDay assetTypeId]} (:params request)]
    (if text
      (-> db
        (create-search-report text
          (parse-id mapId)
          (parse-calendar-day calendarDay)
          (parse-id assetTypeId))
        (to-xml-str))
      (create-bad-request-response request))))

;; asset-id-list and helpers ;;

(defn- get-property-type
  [db property-key]
  (-> (database/get-entities db :asset-types)
    (find-asset-type-property property-key)
    :type))

(defn- create-predicate
  "Returns an asset predicate that returns true iff the value of an asset
  property indicated by the property-key "
  [db property-key search-text calendar-day]
  (create-property-value-predicate-of-type
   (get-property-type db property-key)
   calendar-day
   search-text))

(defn- find-assets
  [db property-key search-text calendar-day]
  (when-let [predicate (create-predicate db property-key search-text calendar-day)]
    (filter
      (fn [asset]
        (predicate (get-asset-property asset property-key)))
      (database/get-entities db :assets))))

(defn- find-engine-asset-ids-by-matching-tags
  [property text]
  (->> (ekahau.vision.engine-service/get-matching-tags text)
    (filter (fn [[_ v]] (= (get v property) text)))
    (map key)
    (set)))

(defn- find-assets-with-matching-tag
  [db property text]
  (let [engine-asset-id-set (find-engine-asset-ids-by-matching-tags property text)]
    (filter
      (fn [asset]
        (engine-asset-id-set (:engine-asset-id asset)))
      (database/get-entities db :assets))))

(defn- asset-id-list-xml
  [id-seq]
  (to-xml-str [:assetIdList {:value (str/str-join "," id-seq)}]))

(defn- asset-id-list
  [db request]
  (let [search-text (-> request :params :text)
        calendar-day (-> request :params :calendarDay parse-calendar-day)
        ^String key-string (-> request :params :key)]
    (if (.startsWith key-string "tag")
      (let [[_ property] (str/re-split #"," key-string)]
        (asset-id-list-xml (map :id (find-assets-with-matching-tag db (keyword property) search-text))))
      (let [property-key (parse-id-string key-string)]
        (or
          (when (and search-text property-key)
            (asset-id-list-xml (map :id (find-assets db property-key search-text calendar-day))))
          (create-bad-request-response request))))))

;; Routes ;;

(defn create-asset-search-routes
  [db]
  (->
    (routes
      (GET "/assets/search/stats" {:as request} (search-stats db request))
      (GET "/assets/search/idList" {:as request} (asset-id-list db request)))
    (with-xml-content-type)))
