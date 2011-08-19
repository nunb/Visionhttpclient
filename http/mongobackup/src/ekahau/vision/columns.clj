(ns ekahau.vision.columns
  (:require
   [ekahau.vision
    [database :as database]
    [uri :as uri]]))

(defn create-total-specification
  [asset-types asset-specs]
  {:type "or"
   :specs (concat
           asset-specs
           (map
            (fn [asset-type]
              {:type "asset-type"
               :asset-type-id (:id asset-type)})
            asset-types))})

(defn column-from-asset-type
  [asset-type]
  (let [{:keys [id name]} asset-type]
    {:name name
     :uri (uri/asset-type-uri id)}))

(defn columns-from-asset-types
  [asset-types]
  (map column-from-asset-type asset-types))

(defn column-from-spec
  [db asset-spec]
  {:name (or (:name asset-spec) (str "Specification " (:id asset-spec)))
   :uri (if-let [id (:id asset-spec)]
          (uri/asset-spec-uri id)
          (uri/asset-spec-uri (format "transients/%s" (:index asset-spec))))})

(defn columns-from-specs
  [db asset-specs]
  (map
   (partial column-from-spec db)
   asset-specs))

(defn get-entities-by-ids
  [db entity-kw ids]
  (map (partial database/get-entity-by-id db entity-kw) ids))

(defn get-asset-types-by-ids
  [db ids]
  (get-entities-by-ids db :asset-types ids))

(defn get-asset-specs-by-ids
  [db ids]
  (get-entities-by-ids db :asset-specs ids))

(defn create-total-column
  [asset-types asset-specs]
  {:type "named"
   :name "Total"
   :id "total"
   :spec (create-total-specification asset-types
                                     asset-specs)})

(defn create-column-specifications
  [db asset-types asset-spec-ids include-total?]
  (let [asset-specs (get-asset-specs-by-ids db asset-spec-ids)]
    (if include-total?
      (concat asset-specs [(create-total-column asset-types asset-specs)])
      asset-specs)))

(defn create-buildings-columns
  [db {asset-type-ids :asset-types
       asset-spec-ids :asset-specs
       include-all? :include-all?
       include-total? :include-total?
       :or {include-all? true
            include-total? true}}]
  (let [asset-types (get-asset-types-by-ids db asset-type-ids)
        asset-specs (create-column-specifications
                     db
                     asset-types
                     asset-spec-ids
                     include-total?)]
    (concat
     (if include-all?
       (concat
        [{:name "All" :uri "all"}]
        (columns-from-asset-types asset-types)
        (columns-from-specs db asset-specs))
       (concat
        (columns-from-asset-types asset-types)
        (columns-from-specs db asset-specs))))))

(defn get-default-site-view-columns
  [db]
  {:asset-types (map :id (database/get-entities db :asset-types))
   :asset-specs []})

(defn get-site-view-columns
  [db user]
  (or (:site-view-columns user)
      (get-default-site-view-columns db)))

(defn create-report-columns
  [db columns]
  (create-buildings-columns db columns))
