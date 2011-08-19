(ns ekahau.vision.predicate
  (:require
   [ekahau.vision.database :as database]
   [ekahau.vision.model.positions :as positions])
  (:use
   [clojure.contrib.trace :only [trace]]
   [ekahau.predicates :only [substring-pred?]]
   [ekahau.string :only [parse-calendar-day]]
   [ekahau.util :only [key-from-property-path]]
   [ekahau.vision.model.asset :only
    [get-asset-property
     create-is-descendant-of-type-asset-predicate]]
   [ekahau.vision.model.zone-grouping :only
    [get-zone-ids-of-zone-group]]))

(defmulti create-asset-predicate (fn [_ asset-spec] (:type asset-spec)))

(defmethod create-asset-predicate "asset-property-value"
  [db asset-spec]
  (fn [asset]
    (if-let [value (get-asset-property asset (:key asset-spec))]
      (= (:value asset-spec) value)
      false)))

(defmethod create-asset-predicate "asset-type"
  [db asset-spec]
  (create-is-descendant-of-type-asset-predicate db (:asset-type-id asset-spec)))

(defn asset-has-id?
  [asset-ids asset]
  (contains? asset-ids (:id asset)))

(defmethod create-asset-predicate "asset-set"
  [db asset-spec]
  (partial asset-has-id? (set (:asset-ids asset-spec))))

(defn create-asset-on-map-predicate
  [db map-id]
  (let [get-map-id (positions/create-get-asset-map-id-fn db)]
    (fn [asset]
      (= map-id (get-map-id asset)))))

(defn create-asset-on-zones-predicate
  [db zone-ids]
  (let [get-zone-id (positions/create-get-asset-zone-id-fn db)]
    (fn [asset]
      (zone-ids (get-zone-id asset)))))

(defmethod create-asset-predicate "true"
  [db asset-spec]
  (constantly true))

(defmethod create-asset-predicate "false"
  [db asset-spec]
  (constantly false))

(defmethod create-asset-predicate "and"
  [db asset-spec]
  (let [predicates (map (partial create-asset-predicate db)
                        (:specs asset-spec))]
    (fn [asset]
      (every? #(% asset) predicates))))

(defmethod create-asset-predicate "or"
  [db asset-spec]
  (let [predicates (map (partial create-asset-predicate db)
                        (:specs asset-spec))]
    (fn [asset]
      (boolean (some #(% asset) predicates)))))

(defmethod create-asset-predicate "not"
  [db asset-spec]
  (let [predicate (create-asset-predicate db (:spec asset-spec))]
    (comp not predicate)))

(defmethod create-asset-predicate "map"
  [db asset-spec]
  (create-asset-on-map-predicate db (:map-id asset-spec)))

(defmethod create-asset-predicate "building"
  [db asset-spec]
  (let [get-building-id (positions/create-get-asset-building-id-fn db)]
    (fn [asset]
      (= (:building-id asset-spec) (get-building-id asset)))))

(defn get-asset-predicate-by-spec-id
  [db spec-id]
  (when-let [asset-spec (database/get-entity-by-id db :asset-specs spec-id)]
    (create-asset-predicate db asset-spec)))

(defmethod create-asset-predicate "asset-spec"
  [db asset-spec]
  (or (get-asset-predicate-by-spec-id db (:asset-spec-id asset-spec))
      (constantly false)))

(defmethod create-asset-predicate "named"
  [db asset-spec]
  (create-asset-predicate db (:spec asset-spec)))

(defmethod create-asset-predicate "zone"
  [db asset-spec]
  (create-asset-on-zones-predicate
   db
   (hash-set (:zone-id asset-spec))))

(defmethod create-asset-predicate "zone-group"
  [db asset-spec]
  (create-asset-on-zones-predicate
   db
   (set
    (get-zone-ids-of-zone-group db
                                (:zone-grouping-id asset-spec)
                                (:zone-group-id asset-spec)))))

(defmethod create-asset-predicate "to-do-list"
  [db asset-spec]
  (if-let [todo-list (database/get-entity-by-id db :todo-lists (:to-do-list-id asset-spec))]
    (partial asset-has-id? (set (map :asset-id (:items todo-list))))
    (constantly false)))

;; ## Text Search Magic

;; ### Property Value Predicates from Text

(defn create-equal-integer-predicate
  "Parses an integer from the text and returns a predicate that returns
  true given an equal integer value. If text can not be parsed as integer
  returns null."
  [text]
  (try
    (let [value (Integer/parseInt text)]
      (partial = value))
    (catch NumberFormatException e nil)))

(defn- create-equal-decimal-number-predicate
  "Parses a double from the text and returns a predicate that returns
  true given an equal double value. If text can not be parsed as double
  returns null."
  [text]
  (try
    (let [value (Double/parseDouble text)]
      (partial = value))
    (catch NumberFormatException e nil)))

(defn- parse-boolean
  "Returns true if s is 'yes' or 'true', false if s is 'no' or 'false'.
  Otherwise returns nil."
  [s]
  (condp contains? (clojure.string/lower-case s)
    #{"yes" "true"} true
    #{"no" "false"} false
    nil))

(defn- create-boolean-predicate
  "Parses a boolean from the text and returns a predicate that returns
  true given an equal value. If text does not represent a boolean value
  nil is returned instead of a predicate function."
  [text]
  (when-let [value (parse-boolean text)]
    (partial = value)))

(defn- create-collection-predicate
  "Returns a predicate that returns true iff text is a substring of the
  given value."
  [text]
  (substring-pred? text))

(defn- create-calendar-day-predicate
  "Returns a predicate that returns true iff given values is an equal
  calendar day."
  [calendar-day]
  (fn [a]
    (= calendar-day a)))

;; ### Asset Predicates

(def predicate-factory-by-property-type
  {"database/text" substring-pred?
   "database/number" create-equal-integer-predicate
   "database/float" create-equal-decimal-number-predicate
   "database/boolean" create-boolean-predicate
   "database/collection" create-collection-predicate})

(defn- append-calendar-day-predicate
  [calendar-day coll]
  (if calendar-day
    (cons ["database/calendar-day" (create-calendar-day-predicate calendar-day)] coll)
    coll))

(defn- create-predicates-per-property-type
  "Creates a map with asset property type keys and predicate values."
  [text calendar-day]
  (->> predicate-factory-by-property-type
    (map (fn [[type f]] [type (f text)]))
    (append-calendar-day-predicate calendar-day)
    (remove (comp nil? second))
    (into {})))

(defn- predicate-per-asset-type-property
  "Creates a map that has asset property keys as keys and value predicates as values."
  [asset-types predicate-per-property-type]
  (into {}
        (for [asset-type asset-types
              property-group (:property-groups asset-type)
              property (:properties property-group)
              :let [predicate (get predicate-per-property-type (:type property))]
              :when predicate]
          [(key-from-property-path [asset-type property-group property])
           predicate])))

(defn get-matching-key-value-pairs
  "Returns a seq of matching asset property key-value pairs where predicate is
  found by property key in predicates and the predicate returns true given the
  corresponding property value."
  [predicates asset]
  (for [[k v :as kv] (:properties asset)
        :let [predicate (get predicates k)]
        :when (and predicate (predicate v))]
    kv))

(defn get-matching-property-value-assets
  [db text calendar-day]
  (let [f (partial get-matching-key-value-pairs
                   (predicate-per-asset-type-property
                    (database/get-entities db :asset-types)
                    (create-predicates-per-property-type text calendar-day)))]
    (mapcat
     (fn [asset]
       (map (fn [[k v]] [k v asset]) (f asset)))
     (database/get-entities db :assets))))

(defn create-property-value-predicate-of-type
  [property-type calendar-day search-text]
  (if (= :calendar-day property-type)
    (create-calendar-day-predicate calendar-day)
    (when-let [create-predicate-fn (get predicate-factory-by-property-type property-type)]
      (create-predicate-fn search-text))))

(defmethod create-asset-predicate "text-search"
  [db asset-spec]
  (trace asset-spec)
  (let [search-string (:search-string asset-spec)
        predicates (predicate-per-asset-type-property
                    (database/get-entities db :asset-types)
                    (create-predicates-per-property-type
                     search-string
                     nil))]
    (fn [asset]
      (some (complement nil?) (get-matching-key-value-pairs predicates asset)))))
