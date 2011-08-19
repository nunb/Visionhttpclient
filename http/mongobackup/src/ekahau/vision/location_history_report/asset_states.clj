(ns ekahau.vision.location-history-report.asset-states
  (:require [ekahau.vision.database :as database])
  (:use [ekahau.vision.model.asset :only [create-ancestor-asset-types-map]])
  (:use [ekahau.vision.predicate :only [create-asset-predicate]]))

(defn create-asset-type-state
  [asset-type-id]
  {:asset-type asset-type-id})

(defn create-get-all-asset-type-ids-fn
  [db]
  (let [asset-type-ancestors (create-ancestor-asset-types-map
                              (database/get-entities db :asset-types))]
    (fn [{asset-type-id :asset-type-id}]
      (cons asset-type-id
            (asset-type-ancestors asset-type-id)))))

(defn create-get-asset-type-states-fn
  [db]
  (let [get-all-asset-type-ids (create-get-all-asset-type-ids-fn db)]
    (fn [asset]
      (set (map create-asset-type-state (get-all-asset-type-ids asset))))))

(defn add-asset-types
  [result db]
  (let [get-asset-type-states (create-get-asset-type-states-fn db)]
    (reduce
     (fn [result asset]
       (assoc result (:id asset) (get-asset-type-states asset)))
     result (database/get-entities db :assets))))

(defn get-asset-spec-predicate-pairs
  [db asset-spec-ids]
  (map
   (fn [id]
     (let [spec (database/get-entity-by-id db :asset-specs id)
           pred (create-asset-predicate db spec)]
       [spec pred]))
   asset-spec-ids))

(defn create-asset-spec-state
  [asset-spec-id]
  {:asset-spec asset-spec-id})

(defn add-asset-specs
  [result db asset-spec-ids]
  (let [asset-spec-predicate-pairs (get-asset-spec-predicate-pairs
                                    db
                                    asset-spec-ids)]
    (reduce
     (fn [result asset]
       (reduce
        (fn [result [spec pred]]
          (if (pred asset)
            (update-in result [(:id asset)]
                       conj (create-asset-spec-state (:id spec)))
            result))
        result asset-spec-predicate-pairs))
     result (database/get-entities db :assets))))

(defn add-all
  [result db]
  (reduce
   (fn [result asset]
     (update-in result [(:id asset)]
                conj (create-asset-spec-state :all)))
   result (database/get-entities db :assets)))

(defn create-total-asset-predicate
  [db asset-type-ids asset-spec-ids]
  (create-asset-predicate
   db
   {:type "or"
    :specs (concat
            (map
             (fn [id]
               {:type "asset-spec"
                :asset-spec-id id})
             asset-spec-ids)
            (map
             (fn [id]
               {:type "asset-type"
                :asset-type-id id})
             asset-type-ids))}))

(defn add-total
  [result db asset-type-ids asset-spec-ids]
  (let [pred (create-total-asset-predicate db asset-type-ids asset-spec-ids)]
    (reduce
     (fn [result asset]
       (if (pred asset)
         (update-in result [(:id asset)]
                    conj (create-asset-spec-state :total))
         result))
     result (database/get-entities db :assets))))

(defn keys-of-matching-predicates
  [key-predicate-entries asset]
  (->> key-predicate-entries
       (filter
        (fn [{:keys [predicate]}]
          (predicate asset)))
       (map :key)
       (set)))

(defn create-states-from-asset-fn-from-key-predicate-entries
  [db key-predicate-entries]
  (->> (database/get-entities db :assets)
       (map
        (juxt :id
              (partial keys-of-matching-predicates key-predicate-entries)))
       (into {})))

(defn create-states-from-asset-fn
  [db asset-type-ids asset-spec-ids]
  (-> {}
      (add-asset-types db)
      (add-asset-specs db asset-spec-ids)))

(defn create-states-from-asset-for-site-view-fn
  "Returns a function (fn [asset-id] ...) that returns all matching states
  based on asset types and asset specifications of an asset with the given id.
  The result includes all and total columns."
  [db asset-type-ids asset-spec-ids]
  (-> (create-states-from-asset-fn db asset-type-ids asset-spec-ids)
      (add-all db)
      (add-total db asset-type-ids asset-spec-ids)))