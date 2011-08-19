(ns ekahau.vision.asset-import
  (:require
   [ekahau.UID]
   [ekahau.vision.database :as database])
  (:use
   [clojure.contrib.condition :only [handler-case raise *condition*]]
   [clojure.contrib.logging :only [info]]
   [clojure.contrib.set :only [subset?]]
   [clojure.contrib.seq-utils :only [indexed]]
   [clojure.contrib.trace :only [trace]]
   [ekahau.excel :only (get-columns load-sheets-as-vectors)]
   [ekahau.string :only [parse-id]]
   [ekahau.vision.model.asset :only (get-asset-property
                                     parse-asset-property-value
                                     get-all-asset-type-property-paths
                                     get-direct-asset-type-property-paths
                                     get-asset-type-property-types)]
   [ekahau.util :only [parse-csv partition-by-counts select-by-id]])
  (:import
   [java.text Format]
   [java.util Calendar]))

(defn- convert-excel-type-to-database-type
  [type]
  (condp = type
    ::ekahau.excel/string  "database/text"
    ::ekahau.excel/boolean "database/boolean"
    ::ekahau.excel/number  "database/float"
    ::ekahau.excel/date    "database/calendar-day"
    "database/text"))

(defn- choose-type
  [types]
  (cond
    (= 0 (count types)) "database/text"
    (= 1 (count types)) (convert-excel-type-to-database-type (first types))
    :else               "database/text"))

(defn- get-suggested-value-type
  [values]
  (choose-type (->> values
                 (map :type)
                 (remove nil?)
                 (distinct))))

(defn- get-calendar-day-from-calendar
  [^Calendar cal]
  [(.get cal Calendar/YEAR)
   (inc (.get cal Calendar/MONTH))
   (.get cal Calendar/DAY_OF_MONTH)])

(defn- get-calendar-day-of-date-using-calendar
  [date ^Calendar calendar]
  (when date
    (get-calendar-day-from-calendar (doto calendar (.setTime date)))))

(defn date->calendar-day
  ([d]
    (get-calendar-day-of-date-using-calendar d (Calendar/getInstance)))
  ([d tz locale]
    (get-calendar-day-of-date-using-calendar d (Calendar/getInstance tz locale))))

(defn- create-properties
  [property-labels columns]
  (map (fn [id label type]
         (struct-map database/property
           :id id
           :label label
           :type type))
       (repeatedly ekahau.UID/gen)
       property-labels
       (map get-suggested-value-type columns)))

(defn- property-group-sizes
  [property-group-header]
  (->> property-group-header
    (indexed)
    (filter second)
    (map first)
    (partition 2 1 [(count property-group-header)])
    (map (fn [[start end]] (- end start)))))

(defn- create-property-group
  [id name property-names columns]
  (struct-map database/property-group
    :id id
    :name name
    :properties (create-properties property-names columns)))

(defn create-property-groups
  [property-group-header property-header columns]
  (if property-group-header
    (let [group-ids (repeatedly ekahau.UID/gen)
          group-names (->> property-group-header (remove nil?) (map :value))
          group-sizes (property-group-sizes property-group-header)
          partitioned-property-names (partition-by-counts group-sizes (map :value property-header))
          partitioned-columns (partition-by-counts group-sizes columns)]
      (map
        create-property-group
        group-ids group-names partitioned-property-names partitioned-columns))
    [(create-property-group 0 "Properties" property-header columns)]))

(defn- create-asset-properties
  [property-keys row]
  (map vector property-keys row))

(defn- create-asset
  [id asset-type-id property-keys row]
  (struct-map database/asset
    :id id
    :asset-type-id asset-type-id
    :properties (create-asset-properties property-keys row)))

(defn- create-assets
  [asset-type asset-ids rows]
  (let [asset-type-id (:id asset-type)
        property-keys (get-all-asset-type-property-paths [asset-type])]
    (map
      (fn [id row]
        (create-asset id asset-type-id property-keys row))
      asset-ids
      rows)))

(defn- get-value-converter
  [type]
  (cond
    (= type "database/calendar-day") date->calendar-day
    :else identity))

(defn- convert-row-data
  [types rows]
  (map
    (fn [row]
      (map
        (fn [type value]
          (if (and (not= ::ekahau.excel/string (:type value))
                (= "database/text" type))
            (if-let [^Format format (:format value)]
              (.format format (:value value))
              (str (:format value)))
            ((get-value-converter type) (:value value))))
        types row))
    rows))

(defn property-group-header-row?
  [row]
  (some nil? row))

(defn property-header-row?
  [row]
  (and
    (first row)
    (<= (count (partition-by nil? row)) 2)))

(defn- trim-headers-and-rows
  [group-header property-header rows]
  (let [property-header (take-while (comp not nil?) property-header)
        col-count (count property-header)]
    [(take col-count group-header)
     property-header
     (map (partial take col-count) rows)]))

(defn get-headers-and-rows
  [sheet-rows]
  (if (property-group-header-row? (first sheet-rows))
    (if (property-header-row? (second sheet-rows))
      (let [[group-header property-header & rows] sheet-rows]
        (trim-headers-and-rows group-header property-header rows))
      (raise :type :invalid-header))
    (if (property-header-row? (first sheet-rows))
      (let [[property-header & rows] sheet-rows]
        (trim-headers-and-rows nil property-header rows))
      (raise :type :invalid-header))))

(defn- create-asset-type
  [db asset-type-name group-header property-header rows]
  (let [columns (get-columns rows)
        property-groups (create-property-groups group-header property-header columns)
        asset-type-id (database/get-next-free-entity-id! db :asset-types)]
    (struct-map database/asset-type
      :id asset-type-id
      :name asset-type-name
      :property-groups property-groups)))

(defn- get-property-types
  [asset-type]
  (for [property-group (:property-groups asset-type)
        property (:properties property-group)]
    (:type property)))

(defn import-assets-of-new-asset-type-from-xls
  [db asset-type-name xls-file]
  (when-first [sheet-rows (load-sheets-as-vectors xls-file)]
    (let [[group-header property-header rows] (get-headers-and-rows sheet-rows)
          asset-type-id (database/get-next-free-entity-id! db :asset-types)
          asset-type (create-asset-type db
                       asset-type-name
                       group-header
                       property-header
                       rows)
          assets (create-assets asset-type
                   (repeatedly ekahau.UID/gen)
                   (convert-row-data (get-property-types asset-type) rows))]
      (-> db
        (database/put-entity! :asset-types asset-type)
        (database/put-entities! :assets assets)))))

(defn- get-property-type
  [db [asset-type-id property-group-id property-id]]
  (->> (database/get-entity-by-id db :asset-types asset-type-id)
    :property-groups
    (filter #(= property-group-id (:id %)))
    (first)
    :properties
    (filter #(= property-id (:id %)))
    (first)
    :type))

(defn- read-csv-as-property-value-tag-id-pairs
  [csvfile property-type]
  (map
    (fn [[property-value-string tag-id-string]]
      [(parse-asset-property-value property-type property-value-string)
       (parse-id tag-id-string)])
    (parse-csv csvfile)))

(defn- asset-index-by-value-of-property
  [assets property-key]
  (into {}
    (->> assets
      (map
        (fn [asset]
          [(get-asset-property asset property-key) asset]))
      (filter first))))

(defn- create-asset-tag-binding-list*
  [assets property-tag-id-pairs property-key]
  (let [asset-index (asset-index-by-value-of-property assets property-key)]
    (->> property-tag-id-pairs
      (map
        (fn [[value tag-id]]
          [(:id (get asset-index value)) tag-id]))
      (filter first))))

(defn create-asset-tag-binding-list
  [db csvfile property-key]
  (create-asset-tag-binding-list*
    (database/get-entities db :assets)
    (read-csv-as-property-value-tag-id-pairs csvfile (get-property-type db property-key))
    property-key))

(defn- parse-property-values
  [types string-values]
  (map #(parse-asset-property-value %1 %2) types string-values))

(defn- put-new-assets
  [db assets]
  (reduce
    (fn [db asset]
      (database/put-entity! db :assets asset))
    db
    assets))

(defn- transform
  [values transformers]
  (map #(%1 %2) transformers values))

(defn import-assets-from-csv
  [db asset-type-id source transformers bind-asset-to-tag]
  (let [[header & records] (parse-csv source)]
    (let [[assets tag-ids]
          (dosync
            (let [database db
                  asset-type (first (select-by-id (:asset-types database) asset-type-id))
                  property-paths (get-direct-asset-type-property-paths asset-type)
                  property-types (get-asset-type-property-types asset-type)
                  transformer-per-field (map (fn [type] (get transformers type identity)) property-types)
                  asset-properties (map #(map vector property-paths (parse-property-values property-types (transform % transformer-per-field)))
                                     records)
                  assets (map #(struct database/asset %1 asset-type-id %2) (iterate inc (database/get-next-free-entity-id! (:assets database))) asset-properties)
                  tag-ids (map (comp parse-id last) records)]
              (put-new-assets db assets)
              [assets tag-ids]))]
      (doseq [[asset tag-id] (map vector assets tag-ids)]
        (bind-asset-to-tag (:id asset) tag-id)))))
