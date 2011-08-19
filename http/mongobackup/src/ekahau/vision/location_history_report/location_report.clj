(ns ekahau.vision.location-history-report.location-report
  (:require
   [ekahau.vision.database :as database])
  (:use
   [clojure.contrib.trace
    :only [trace]]
   [ekahau.vision.location-history-report.base-report]
   [ekahau.vision.predicate
    :only [create-asset-predicate
           get-asset-predicate-by-spec-id]]
   [ekahau.vision.json.asset-spec
    :only [parse-json-asset-spec]]
   [ekahau.vision.columns
    :only [column-from-asset-type
           column-from-spec]]
   [ekahau.vision.routes.building
    :only [create-buildings-values
           create-site-hierarchy
           rows-from-site-hierarchy
           site-nodes-in-preorder]]))

(def *statistic-columns* [:min-concurrent-visitors
                          :max-concurrent-visitors
                          :total-visitors
                          :visit-count
                          :non-visitor-count
                          :average-visit-time
                          :average-total-time
                          :total-visit-time])

(defn index-columns
  [specs]
  (map-indexed #(assoc-in %2 [:spec :index] %1) specs))

(defn index-columns-of-report
  "Adds an index number to each asset-spec in :columns-request. Indices are
  used to create an unique URI within report for each column."
  [report]
  (update-in report [:columns-request] index-columns))

(defn uri-from-spec
  [spec]
  (str "assetSpecs/transients/" (get spec :index)))

(defn selected-statistic-columns-from-spec
  [spec]
  (select-keys spec *statistic-columns*))

(defn selected-column-from-spec
  [{:keys [spec statistics-columns]}]
  [(uri-from-spec spec)
   (selected-statistic-columns-from-spec statistics-columns)])

(defn parse-selected-columns
  [report]
  (map selected-column-from-spec (:columns-request report)))

(defn update-selected-columns-based-on-columns-request
  [report]
  (assoc report :selected-columns (parse-selected-columns report)))

(defmethod prepare-report "location"
  [report]
  (-> report
      (index-columns-of-report)
      (update-selected-columns-based-on-columns-request)))


;; Report Columns ;;

;; ... actual report columns (:name and :uri entries)

(defn create-column
  "Returns a column for the give column specification. The result is a map
  with keys :name and :uri."
  [db column-spec]
  (column-from-spec db (:spec column-spec)))

(defn create-columns
  [db column-specifications]
  (map
   (partial create-column db)
   column-specifications))

;; ... keys

(defn column-key-from-spec
  [asset-spec]
  {:asset-spec (format "transients/%s" (:index asset-spec))})

(defn columns-keys-from-column-specs
  [specs]
  (map (comp column-key-from-spec :spec) specs))

;; ... predicates

(defn create-key-predicate-entries-from-column-specs
  [db column-specs]
  (map
   (fn [{:keys [spec]}]
     {:key (column-key-from-spec spec)
      :predicate (create-asset-predicate db (parse-json-asset-spec spec))})
   column-specs))


;; Final Report ;;

(defn get-buildings-with-column-specifications
  [db column-specifications get-cell-value]
  (dosync
   (let [columns (create-columns db column-specifications)
         column-keys (columns-keys-from-column-specs column-specifications)
         site-hierarchy (create-site-hierarchy db)
         row-keys (map :key (site-nodes-in-preorder site-hierarchy))]
     {:rows (rows-from-site-hierarchy site-hierarchy)
      :columns columns
      :values (create-buildings-values
               row-keys
               column-keys
               get-cell-value)})))
