(ns ekahau.vision.model.asset
  (:require
   [ekahau.vision.database :as database]
   [ekahau.UID :as uid])
  (:use
   [clojure.set :only (select join rename)]
   [clojure.contrib.seq-utils :only (find-first)]
   [ekahau.predicates :only (value-by-key-in-set-pred?)]
   [ekahau.util :only (has-id?
                       assoc-multi
                       get-descendants
                       key-from-property-path 
                       assign-ids)]))

(def get-asset-type-by-id (database/create-get-by-id-fn :asset-types))
(def get-asset-by-id (database/create-get-by-id-fn :assets))

(defn get-assets
  [db]
  (database/get-entities db :assets))

(defn get-engine-asset-ids
  [db]
  (->> (get-assets db)
    (filter :engine-asset-id)
    (map :engine-asset-id)))

(defn get-asset-types
  [db]
  (database/get-entities db :asset-types))

(defn get-asset-property
  [asset property-key]
  (some (fn [[k v]] (when (= k property-key) v))
    (:properties asset)))

(defn has-engine-asset-id?
  [asset]
  (:engine-asset-id asset))

(defn- find-first-with-id 
  [coll id]
  (find-first #(has-id? % id) coll))

(defn- find-in-tree
  [coll id key-id-pairs]
  (loop [coll coll id id key-id-pairs key-id-pairs]
    (let [elem (find-first-with-id coll id)]
      (if (not (seq key-id-pairs))
        elem
        (let [[k v] (first key-id-pairs)]
          (recur (k elem) v (rest key-id-pairs)))))))

(defn find-asset-type-property
  [asset-types [asset-type-id property-group-id property-id]]
  (find-in-tree asset-types asset-type-id [[:property-groups property-group-id] [:properties property-id]]))

;; ## Asset Types

;; ### Hierarchy

(defn index-asset-types-by-id
  [asset-types]
  (into {} (zipmap (map :id asset-types) asset-types)))

(defn create-parent-asset-type-map
  [asset-types]
  (->> asset-types
       (map (juxt :id :parent-type-id))
       (into {})))

(defn get-ancestor-asset-types
  [parent-asset-type-map id]
  (when-let [parent-id (get parent-asset-type-map id)]
    (cons
     parent-id
     (get-ancestor-asset-types parent-asset-type-map parent-id))))

(defn create-ancestor-asset-types-map
  [asset-types]
  (let [get-ancestors (partial get-ancestor-asset-types
                               (create-parent-asset-type-map asset-types))]
    (->> asset-types
         (map (fn [{id :id}]
                [id (vec (get-ancestors id))]))
         (into {}))))

(defn get-asset-type-and-ancestors
  [db asset-type-id]
  (when asset-type-id
    (when-let [asset-type (get-asset-type-by-id db asset-type-id)]
      (lazy-seq
        (cons asset-type (get-asset-type-and-ancestors db (:parent-type-id asset-type)))))))

(defn get-all-asset-types
  [db asset-type-id]
  (reverse (get-asset-type-and-ancestors db asset-type-id)))

;; ### Printing

(defn- print-asset-of-types
  [asset asset-types]
  (doseq [asset-type asset-types]
    (println (:name asset-type))
    (doseq [group (:property-groups asset-type)]
      (println " " (:name group))
      (doseq [property (:properties group)]
        (println "  " (str (:label property) ":") (get-asset-property asset
                                                    (map :id [asset-type group property])))))))

;; ## Assets

(defn print-asset
  [db id]
  (when-let [asset (get-asset-by-id db id)]
    (let [asset-types (get-all-asset-types db (:asset-type-id asset))]
      (print-asset-of-types asset asset-types))))

(defn get-asset-by-engine-asset-id
  [db engine-asset-id]
  (when engine-asset-id
    (first (database/search-entities db :assets
             [["=" [:engine-asset-id] engine-asset-id]]
             {:limit 1}))))

(defn get-type-of-asset-property 
  [asset-types property-key]
  (:type (find-asset-type-property asset-types property-key)))

(defn parse-asset-property-value
  [type val-str]
  (if-let [parse-value (get database/property-value-parsers type)]
    (when (< 0 (count val-str)) (parse-value val-str))
    (throw (IllegalArgumentException. (str "Unknown type in asset property value: " type)))))

(defn- find-property-group
  [property-groups asset-type-id property-group-id]
  (filter #(and (= asset-type-id (:asset-type-id %)) (= property-group-id (:id %))) property-groups))

(defn get-type-of-asset-property-from-property-groups
  [property-groups [asset-type-id property-group-id property-id]]
  (when-first [property (filter #(= property-id (:id %)) (mapcat :properties (find-property-group property-groups asset-type-id property-group-id)))]
    (:type property)))

(defn get-asset-type-property-paths-from-asset-types
  [asset-types]
  (for [asset-type asset-types
        property-group (:property-groups asset-type)
        property (:properties property-group)]
    [asset-type property-group property]))

(defn- create-property-id-to-property-path-map
  [asset-types]
  (let [paths (get-asset-type-property-paths-from-asset-types asset-types)]
    (apply hash-map (interleave (map (comp vec (partial map :id)) paths) paths))))

(defn create-get-property-type-by-id-fn
  [asset-types]
  (let [property-id-to-property-path-map (create-property-id-to-property-path-map asset-types)]
    (fn [id]
      (let [[_ _ property] (property-id-to-property-path-map id)]
        (:type property)))))

(defn get-asset-type-property-path
  [asset-types asset-type-name property-group-name property-label]
  (first
    (for [asset-type asset-types                       :when (= asset-type-name (:name asset-type))
          property-group (:property-groups asset-type) :when (= property-group-name (:name property-group))
          property (:properties property-group)        :when (= property-label (:label property))]
      (key-from-property-path [asset-type property-group property]))))

(defn- find-assets-by-property-predicate
  [assets property-key pred]
  (filter
    (fn [asset]
      (pred (get-asset-property asset property-key)))
    assets))

(defn find-assets-by-property-value
  [assets property-key value]
  (find-assets-by-property-predicate assets property-key #(= % value)))

(defn- matches-property-key?
  [k1 k2]
  "Returns true if each corresponding values are equal or if value of first path is nil."
  (every? true? (map (fn [a b] (or (nil? a) (= a b))) k1 k2)))

(defn- has-asset-property-value?
  [asset property-path-spec value]
  (let [properties (:properties asset)]
    (some
      (fn [[k v]]
        (and
          (matches-property-key? property-path-spec k)
          (= (get-asset-property asset k) value)))
      properties)))

(defn find-assets-by-property-key
  [assets property-path value]
  (filter #(has-asset-property-value? % property-path value) assets))

(defn- get-all-asset-property-values
  "Returns a seq of all values of a specified property."
  [assets property-path]
  (map #(get-asset-property % property-path) assets))


(defn- get-asset-type-property-elements
  [asset-types [asset-type-id property-group-id property-id]]
  (when-first [asset-type (filter #(has-id? % asset-type-id) asset-types)]
    (when-first [property-group (filter #(has-id? % property-group-id) (:property-groups asset-type))]
      (when-first [property (filter #(has-id? % property-id) (:properties property-group))]
        [asset-type property-group property]))))

(defn- get-asset-type-property-path-names
  [asset-types property-path]
  (when-let [property-elements (get-asset-type-property-elements asset-types property-path)]
    (let [[asset-type property-group property] property-elements]
      [(:name asset-type) (:name property-group) (:label property)])))

(defn- has-asset-type-property-path-value-type?
  [property-path type]
  (let [[_ _ property] property-path]
    (= (:type property) type)))

(defn get-all-asset-type-property-paths
  [asset-types]
  (map
    key-from-property-path
    (get-asset-type-property-paths-from-asset-types asset-types)))

(defn get-direct-asset-type-property-paths
  [asset-type]
  (get-all-asset-type-property-paths [asset-type]))

(defn- direct-child-asset-type-pred?
  [asset-type-id]
  (fn [asset-type]
    (= asset-type-id (:parent-type-id asset-type))))

(defn- get-all-asset-type-parent-child-pairs
  [asset-types]
  (for [parent asset-types :let [child? (direct-child-asset-type-pred? (:id parent))]
        child asset-types :when (child? child)]
    [parent child]))

(defn- create-child-asset-types-map
  [asset-types]
  (reduce
    (fn [result [parent child]]
      (assoc-multi result (:id parent) child []))
    {}
    (get-all-asset-type-parent-child-pairs asset-types)))

(defn create-get-child-asset-types-fn
  [asset-types]
  (let [child-asset-types (create-child-asset-types-map asset-types)]
    (fn [asset-type]
      (get child-asset-types (:id asset-type)))))

(defn- create-get-asset-type-descendants-fn
  [asset-types]
  (let [get-children (create-get-child-asset-types-fn asset-types)]
    (fn [asset-type]
      (get-descendants asset-type get-children))))

(defn- get-asset-type-descendants
  [asset-types asset-type]
  ((create-get-asset-type-descendants-fn asset-types) asset-type))

(defn- create-asset-of-any-given-type-pred
  [asset-types]
  (->> asset-types
       (map :id)
       (set)
       (value-by-key-in-set-pred? :asset-type-id)))

(defn create-asset-instance-of-type-predicate
  [asset-types asset-type]
  (create-asset-of-any-given-type-pred
   (cons asset-type
         (get-asset-type-descendants asset-types asset-type))))

(defn- get-all-descendants-of-given-asset-types
  [all-asset-types given-asset-types]
  (->> given-asset-types
       (mapcat (create-get-asset-type-descendants-fn all-asset-types))
       (set)))

(defn create-asset-instance-of-any-given-type
  [all-asset-types given-asset-types]
  (create-asset-of-any-given-type-pred
   (concat given-asset-types
           (get-all-descendants-of-given-asset-types
            all-asset-types given-asset-types))))

(defn get-asset-type-and-descendants
  [db asset-type-id]
  (when-let [asset-type (get-asset-type-by-id db asset-type-id)]
    (cons asset-type (get-asset-type-descendants (get-asset-types db) asset-type))))

(defn get-asset-types-for-asset
  [db id]
  (get-all-asset-types db (:asset-type-id (get-asset-by-id db id))))

(defn create-is-descendant-of-type-asset-predicate
  [db asset-type-id]
  (->> (get-asset-type-and-descendants db asset-type-id)
       (map :id)
       (set)
       (value-by-key-in-set-pred? :asset-type-id)))

(defn get-assets-of-type-and-descendants
  [db asset-type-id]
  (select
    (create-is-descendant-of-type-asset-predicate db asset-type-id)
    (set (get-assets db))))

(defn get-asset-type-property-groups
  "Returns asset type property groups in order start from the most distant ancestor.
  Each property group has an asset-type-id value of the type where the group if
  defined."
  [db asset-type-id]
  (when-let [asset-types (get-all-asset-types db asset-type-id)]
    (mapcat
      (fn [{:keys [id property-groups]}]
        (map #(assoc % :asset-type-id id) property-groups))
      asset-types)))

(defn- get-asset-type-property-paths
  "Returns all asset type property paths starting from to root type properties."
  [db asset-type-id]
  (get-asset-type-property-paths-from-asset-types (get-all-asset-types db asset-type-id)))

(defn get-asset-type-property-values
  "Returns all values of the specified property of all assets, including nils
  in when asset does not have a value assinged for the property."
  [asset-types assets [asset-type-id _ _ :as property-path]]
  (when-first [asset-type (filter #(has-id? % asset-type-id) asset-types)]
    (let [asset-instance-of-type? (create-asset-instance-of-type-predicate asset-types asset-type)]
      (get-all-asset-property-values (filter asset-instance-of-type? assets) property-path))))

(defn- get-all-values-per-asset-type-property
  [asset-types assets]
  (reduce
    (fn [results property-path]
      (if-let [values (get-asset-type-property-values asset-types assets property-path)]
        (assoc results property-path values)
        results))
    {} (get-all-asset-type-property-paths asset-types)))

(defn- get-value-counts-per-asset-type-property
  "Returns a map from property path to a map from value to count."
  [asset-types assets]
  (reduce
    (fn [results [k v]]
      (assoc results k (frequencies v)))
    {}
    (get-all-values-per-asset-type-property asset-types assets)))

(defn get-asset-type-property-types
  [asset-type]
  (mapcat #(map :type (:properties %)) (:property-groups asset-type)))

(defn get-asset-type-value-satistics
  [db asset-type-id]
  (let [asset-types (get-asset-types-for-asset db asset-type-id)
        assets (get-assets db)
        value-counts (get-value-counts-per-asset-type-property asset-types assets)]
    {:asset-types
     (map
       (fn [asset-type]
         {
          :asset-type asset-type
          :property-groups
          (map
            (fn [property-group]
              {
               :property-group property-group
               :properties
               (map
                 (fn [property]
                   {
                    :property property
                    :values (get value-counts (key-from-property-path [asset-type property-group property]))
                    })
                 (:properties property-group))
               })
            (:property-groups asset-type))  
          })
       asset-types)}))

(defn- get-asset-type-card-for-asset
  [db asset]
  (when-let [asset-type-id (:asset-type-id asset)]
    (first (map :card-desc (get-all-asset-types db asset-type-id)))))

(defn get-default-asset-type-card
  [types]
  (when-first [path (get-asset-type-property-paths-from-asset-types types)]
    (let [[type property-group _] path
          title-key (key-from-property-path path)]
      (struct database/card-desc
        title-key
        (->> (:properties property-group)
          (rest)
          (map (fn [property]
                 (key-from-property-path [type property-group property]))))))))

(defn get-asset-type-card
  [db asset-type-id]
  (when-let [types (get-all-asset-types db asset-type-id)]
    (or
      (some :card-desc (reverse types))
      (get-default-asset-type-card types))))


(defn- get-primary-asset-property-key
  [db asset]
  (if-let [asset-type-card (get-asset-type-card-for-asset db asset)]
    (:title-key asset-type-card)
    (-> (get-asset-type-property-paths db (:asset-type-id asset))
      (first)
      (key-from-property-path))))

(defn get-short-asset-string
  [db asset]
  (if-let [property-key (get-primary-asset-property-key db asset)]
    (str (get-asset-property asset property-key))))

(defn- group-assets-of-type-by-property-value-of-key
  [db asset-type-id property-key]
  (->> (get-assets db)
    (filter #(= asset-type-id (:asset-type-id %)))
    (group-by #(get-asset-property % property-key))))

(defn list-assets-by-asset-type
  [db]
  (map (fn [asset-type]
         {:asset-type asset-type
          :assets (get-assets-of-type-and-descendants db (:id asset-type))})
    (get-asset-types db)))

(defn join-assets-to-position-observations
  [assets asset-position-observations]
  (join
    (select :engine-asset-id (set assets))
    (rename (set (map #(into {} %) asset-position-observations)) {:id :engine-asset-id})))

(defn assign-property-group-ids
  [property-groups]
  (doall
   (map #(update-in % [:properties] assign-ids (repeatedly uid/gen))
        (assign-ids property-groups (repeatedly uid/gen)))))

(defn preorder-asset-type-traversal
  [asset-types]
  (let [asset-types-by-parent-id (group-by :parent-type-id asset-types)]
    ((fn this [parent-id level]
       (let [children (sort-by :name (get asset-types-by-parent-id parent-id))]
         (mapcat
           (fn [child]
             (cons (assoc child :level level) (this (:id child) (inc level))))
           children)))
      nil 0)))
