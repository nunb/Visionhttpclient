(ns ekahau.vision.routes.asset-import
  (:require
   [ekahau.vision.database :as database])
  (:use
   [clojure.xml :only [parse]]
   compojure.core
   [clojure.contrib.condition :only [raise handler-case]]
   [clojure.contrib.duck-streams :only [copy]]
   [clojure.contrib.java-utils :only [delete-file]]
   [clojure.contrib.trace :only [trace]]
   [ekahau.util :only (select-by-id)]
   [ekahau.predicates :only [id-pred?]]
   [ekahau.vision.asset-import :only [import-assets-of-new-asset-type-from-xls]]
   [ekahau.vision.model.asset :only (get-asset-property
                                     get-all-asset-types
                                     get-all-asset-type-property-paths)]
   [ekahau.vision.routes.asset :only [read-assets]]
   [ekahau.vision.routes.helpers
    :only [create-bad-request-response
           create-resource-not-found-response
           parse-id-string
           parse-request-route-id
           with-xml-content-type]]
   [ekahau.string :only [parse-id]]
   [ekahau.xml :only [to-xml-str]])
  (:import
   [java.io File]))

;; Import ;;

(defn- file-suffix
  [^String filename]
  (when filename
    (let [suffix-period-index (.lastIndexOf filename ".")]
      (when (<= 0 suffix-period-index)
        (.substring filename (inc suffix-period-index))))))

(defn- split-filename-suffix
  [^String filename]
  (when filename
    (let [suffix-index (.lastIndexOf filename ".")]
      (if (<= 0 suffix-index)
        [(.substring filename 0 suffix-index)
         (.substring filename (inc suffix-index))]
        filename))))

(defn- create-temp-copy
  [src prefix suffix]
  (let [f (File/createTempFile prefix (str "." suffix))]
    (copy src f)
    f))

(defn properties-equals-by-value?
  [a b]
  (apply = (map #(dissoc (into {} %) :id) [a b])))

(defn- property-groups-equals-by-value?
  [group1 group2]
  (and
    (= (:name group1) (:name group2))
    (every? true? (map properties-equals-by-value? (:properties group1) (:properties group2)))))

(defn- asset-types-compatible?
  [a-groups b-groups]
  (and
    (= (count a-groups) (count b-groups))
    (every? true? (map property-groups-equals-by-value? a-groups b-groups))))

(defn- get-asset-type-lineage
  [asset-types asset-type]
  (let [get-parents (fn this [asset-type]
                      (when-first [parent (filter (id-pred? (:parent-type-id asset-type)) asset-types)]
                        (cons parent (this parent))))]
    (reverse (cons asset-type (get-parents asset-type)))))

(defn- get-applicable-asset-types
  [target-asset-types source-asset-types]
  (for [source-asset-type source-asset-types
        target-asset-type target-asset-types
        :let [lineage (get-asset-type-lineage target-asset-types target-asset-type)
              target-groups (mapcat :property-groups lineage)]
        :when (asset-types-compatible? (:property-groups source-asset-type) target-groups)]
    target-asset-type))

(defn- create-import!
  [db imports-ref request]
  {:pre [(:multipart-params request)]}
  (let [{:keys [filename tempfile]} (-> request :params :Filedata)]
    (let [data-file-suffix (file-suffix filename)
          import-file (create-temp-copy tempfile "asset-import" data-file-suffix)]
      (dosync
        (let [import-db (import-assets-of-new-asset-type-from-xls
                          (database/create-in-memory-vision-database)
                          (first (split-filename-suffix filename))
                          import-file)
              import {:id (ekahau.UID/gen)
                      :file import-file
                      :db import-db
                      :asset-types (get-applicable-asset-types
                                     (database/get-entities db :asset-types)
                                     (database/get-entities import-db :asset-types))}]
          (alter imports-ref conj import)
          import)))))

(defn- update-property-key-asset-type-id
  [property-key asset-type-id]
  (assoc property-key 0 asset-type-id))

(defn- update-property-keys-asset-type-id
  [asset get-target-property-key]
  (update-in asset [:properties]
             (fn [properties]
               (vec
                (map (fn [[k v]]
                       [(get-target-property-key k) v])
                     properties)))))

(defn- update-asset-type
  [asset asset-type-id get-target-property-key]
  (-> asset
    (assoc :asset-type-id asset-type-id)
    (update-property-keys-asset-type-id get-target-property-key)))

(defn- put-entity-if-not-nil
  [db entity-kw entity]
  (if entity
    (database/put-entity! db entity-kw entity)
    db))

(defn- get-import-asset-type
  [db import asset-type]
  (if-let [id (:id asset-type)]
    (database/get-entity-by-id db :asset-types id)
    (merge (->> (database/get-entities (:db import) :asset-types)
                first
                (database/assoc-new-entity-id! db :asset-types))
           (select-keys asset-type [:name :icon]))))

(defn- get-matching-pairs
  [coll1 coll2 pred]
  (for [a coll1
        b coll2 :when (pred a b)]
    [a b]))

(defn- assets-match-by-key?
  [a b key]
  (let [[av bv] (map #(get-asset-property % key) [a b])]
    (= av bv)))

(defn- assets-match-by-key-pred?
  [key]
  (fn [a b]
    (assets-match-by-key? a b key)))

(defn- create-get-target-property-key-transform-asset-type
  [asset-type-id]
  (fn [property-key]
    (assoc property-key 0 asset-type-id)))

(defn- get-property-key-mapping
  [import-db target-db asset-type-id]
  (let [source-type (first (database/get-entities import-db :asset-types))
        target-type (first (get-all-asset-types target-db asset-type-id))]
    (if target-type
      (apply zipmap (map get-all-asset-type-property-paths [[source-type] [target-type]]))
      (create-get-target-property-key-transform-asset-type asset-type-id))))

(defn- assets-from-import-db-with-ids-converted-to-target-db
  [import-db target-db asset-type-id get-target-property-key]
  (->> (database/get-entities import-db :assets)
       (map #(update-asset-type %
                                asset-type-id
                                get-target-property-key))))

(defn- split-assets
  [asset-type target-db import-db id-property-key]
  (let [asset-type-id (:id asset-type)
        get-target-property-key (get-property-key-mapping
                                 import-db
                                 target-db
                                 asset-type-id)
        import-assets (assets-from-import-db-with-ids-converted-to-target-db
                        import-db
                        target-db
                        asset-type-id
                        get-target-property-key)]
    (if id-property-key
      (let [target-assets (database/get-entities target-db :assets)
            pairs (get-matching-pairs
                   import-assets
                   target-assets
                   (assets-match-by-key-pred?
                    (get-target-property-key id-property-key)))
            update-import-asset-set (set (map first pairs))
            update-target-asset-set (set (map second pairs))]
        (when-not (apply = (map count
                                [pairs
                                 update-import-asset-set
                                 update-target-asset-set]))
          (raise :type :ambiguous-join))
        (let [new-assets (clojure.set/difference (set import-assets)
                                                 update-import-asset-set)
              updated-assets (map (fn [[imported target]]
                                    (assoc imported :id (:id target)))
                                  pairs)]
          [new-assets updated-assets]))
      [import-assets nil])))

(defn- commit-import
  [db import asset-type id-property-key]
  (let [import-db (:db import)
        asset-type (get-import-asset-type db import asset-type)
        [new-assets updated-assets] (split-assets asset-type db import-db id-property-key)
        new-assets (database/assoc-new-entity-ids! db :assets new-assets)]
    (-> db
      (put-entity-if-not-nil :asset-types asset-type)
      (database/put-entities! :assets updated-assets)
      (database/put-entities! :assets new-assets))))

(defn- clear-import!
  [import]
  (when import
    (delete-file (:file import) true)
    nil))

(defn- dispatch-clear-import
  [import]
  (send-off (agent import) clear-import!))

(defn- commit-import!
  [db imports-ref id asset-type id-property-key]
  (dosync
    (if-let [import (first (select-by-id @imports-ref id))]
      (do
        (commit-import db import asset-type id-property-key)
        (alter imports-ref disj import)
        (dispatch-clear-import import)
        nil)
      (raise :type :resource-not-found))))

(defonce *imports-ref* (ref #{}))

(defn- get-import
  [request imports-ref]
  (when-let [id (parse-request-route-id request)]
    (first (select-by-id @imports-ref id))))

(defn- get-import-asset-types
  [request imports-ref]
  (or
    (when-let [import (get-import request imports-ref)]
      (->
        (into
          [:assetTypes]
          (map
            (fn [asset-type] [:assetType (select-keys asset-type [:id :name :icon])])
            (:asset-types import)))
        (to-xml-str)))
    (create-resource-not-found-response request)))

(defn- delete-asset-import!
  [imports-ref id]
  (dosync
    (let [deleted-imports (select-by-id @imports-ref id)]
      (if (seq deleted-imports)
        (do
          (alter imports-ref clojure.set/difference deleted-imports)
          (dispatch-clear-import (first deleted-imports))
          (to-xml-str [:deleted]))
        (raise :type :resource-not-found)))))

(defn create-create-import-routes
  [db]
  (->
   (routes
    (POST "/assetImports" {:as request}
      (let [import (create-import! db *imports-ref* request)]
        (to-xml-str [:import {:id (:id import)}]))))
   (with-xml-content-type)))

(defn create-asset-import-routes
  ([db]
     (create-asset-import-routes db *imports-ref*))
  ([db imports-ref]
     (->
      (routes
       (GET "/assetImports/:id/assets" {:as request}
         (or
          (when-let [import (get-import request imports-ref)]
            (read-assets (:db import) request))
          (create-resource-not-found-response request)))
       (GET "/assetImports/:id/assetTypes" {:as request}
         (get-import-asset-types request imports-ref))
       (POST "/assetImports/:id/commit" {:as request}
         (or
          (when-let [id (parse-request-route-id request)]
            (let [body (parse (:body request))
                  body-attrs (:attrs body)
                  asset-type-id (parse-id (:assetTypeId body-attrs))
                  asset-type-name (:assetTypeName body-attrs)
                  asset-type-icon (:assetTypeIcon body-attrs)
                  id-property-key (parse-id-string (:idPropertyKey body-attrs))]
              (handler-case :type
                (commit-import! db imports-ref id
                                (struct-map database/asset-type
                                  :id asset-type-id
                                  :name asset-type-name
                                  :icon asset-type-icon)
                                id-property-key)
                (to-xml-str [:commit])
                (handle :resource-not-found
                  (create-resource-not-found-response request))
                (handle :ambiguous-join
                  (create-bad-request-response
                   request
                   [:error {:type "ambiguous.join"}]
                   to-xml-str)))))
          (create-resource-not-found-response request)))
       (POST "/assetImports/:id" {:as request}
         (let [id (parse-request-route-id request)
               body (parse (:body request))]
           (condp = (-> body :attrs :_method)
               "DELETE" (delete-asset-import! imports-ref id)
               (create-resource-not-found-response request)))))
      (with-xml-content-type))))
