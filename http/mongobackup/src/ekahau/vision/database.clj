(ns ekahau.vision.database
  (:require
    [ekahau.db.in-memory :as in-mem]
    [ekahau.db.mongodb :as mongodb])
  (:use
    [clojure.set :only [union]]
    [clojure.contrib.logging :only [error]]
    [ekahau.util :only [get-multifns]]
    [ekahau.string :only [parse-boolean parse-number parse-double parse-id parse-calendar-day]]))

;; == Vision Structures == ;;

(def property-value-parsers {
                             "database/text"         identity
                             "database/number"       parse-number
                             "database/float"        parse-double
                             "database/calendar-day" parse-calendar-day
                             "database/boolean"      parse-boolean
                             "database/collection"   identity
                             })

(defn name-to-db-type
  [name]
  (str "database/" name))

(defn db-type-to-name
  [^String dbtype]
  (.substring dbtype 9))

;; Model ;;
(defstruct model :id :active)

;; Building ;;

(defstruct building :id :name)
(defstruct floor :id :building-id :map-id :order-num)

;; Map ;;

(defstruct scale :value :unit)
(defstruct model-map :id :name :image :scale)

;; Zone ;;

(defstruct area :map-id :polygon)
(defstruct zone :id :name :area)

;; Asset Type ;;

(defstruct asset-type :id :version :parent-type-id :name :icon :description :property-groups :card-desc :engine-asset-group-id)

(defstruct property-group :id :name :properties)

(defstruct property :id :label :type)

(defstruct card-desc :title-key :subtitle-keys :field-key-rows)

;; Asset ;;

; :tag-id & :mac are purely for debugging purposes
(defstruct asset :id :asset-type-id :properties :engine-asset-id :tag-id :mac)

;; position ;;

(defstruct position :map-id :point :zone-id :model-id)
(defstruct detailed-position :map :point :model
                             :floor :building :site
                             :zone :zone-grouping)
(defstruct position-observation :position :timestamp)
(defstruct entity-position-observation :id :position-observation)

;; Route ;;

(defstruct route :tag-id :position-observations)
(defstruct route-point :engine-asset-id :pos-obs :timetoscan)

;; Status ;;

(defstruct status-type :id :name :values)
(defstruct status-value :key :label :color)

;; Zone Grouping ;;

(defstruct zone-grouping :id :name :version :groups)
(defstruct zone-group :id :name :color :zone-ids)

;; Event Rule ;;

(defstruct event-rule
  :id :version :active? :disabled? :name :description :engine-id
  :trigger :action :knowledge-begin-time :knowledge-end-time)

;; Event ;;

(defstruct event
  :id :type :event-rule-info :asset-info :asset-type-info
  :engine-asset-id :engine-tag-id :position-observation :timestamp
  :actions :msg-recp :accepted-by :declined-by :comments
  :closed? :closing-time :closing-remarks)

;; Mailbox ;;

(defstruct message :id :type :event-id :message :read-by :recipients :timestamp)

;; TO-DO list ;;

(defstruct todo-list :id :name :description :due-date :users :items)

(defstruct item :asset-id :checked-off? :check-off-time)

;; Tables / Collections ;;

(defonce *vision-persistent-tables*
  #{:assets
    :asset-specs
    :asset-types
    :events
    :event-rules-history
    :messages
    :par-history
    :par-history-fetch-timestamps
    :todo-lists
    :reports
    :users
    :zones
    :zone-groupings})

(defonce *vision-non-persistent-tables*
  #{:maps
    :floors
    :buildings
    :models
    :status-types
    :asset-position-observations})

(defonce *vision-tables*
  (union
    *vision-persistent-tables*
    *vision-non-persistent-tables*))

;; Default data that should be present ;;

(def *default-asset-types*
  #{(struct-map asset-type
      :id "Person"
      :name "People"
      :icon "user1.png"
      :property-groups [(struct-map property-group
                          :id "0"
                          :name "Identification"
                          :properties [(struct-map property
                                         :id "0"
                                         :label "Name"
                                         :type "database/text")])])
    (struct-map asset-type
      :id "Asset"
      :name "Assets"
      :icon "package.png"
      :property-groups [(struct-map property-group
                          :id "0"
                          :name "Identification"
                          :properties [(struct-map property
                                         :id "0"
                                         :label "Serial Number"
                                         :type "database/text")])])})

(def *default-zone-groupings*
  #{(struct-map zone-grouping :id "Default" :name "Default" :version 0 :groups [])})

;; == Database Core == ;;

(defn- database-type
  "Dispatch function that returns the tag property of the first parameter."
  [db & args]
  (:tag db))

(defmulti create-database!
  "[db-env persistent-tables non-persistent-tables]
   This function creates (and returns) the database handle/object/instance."
  database-type)

(defmulti valid-table?
  "[db entity-kw]
   Checks if a table is valid (can be used?)"
  database-type)

(defmulti db-type
  "[db entity-kw]
   Gives the db-type of a table.
   Mainly used in giving specific search queries for mongodb."
  database-type)

(defmulti get-table-names
  "[db]
   Gives the list of all table names (entity kws) in the database"
  database-type)

(defmulti get-next-free-entity-id!
  "[db entity-kw]
   Returns:
     An id which is distinct from any other id in the table.
   Will never return the same id twice, regardless called concurrently
   from any number of threads."
  database-type)

(defmulti assoc-new-entity-id!
  "[db entity-kw entity]
   If the entity already has :id,
     Makes sure that get-next-free-entity-id! never returns that 
   Else,
     Assocs a new :id using get-next-free-entity-id!
   Returns:
     Entity with :id"
  database-type)

(defmulti put-entity!
  "[db entity-kw entity]
   1) If the entity doesn't contain an :id, a new :id is attached
      to it using get-next-free-entity-id!. You may choose to do so yourself
      if you want to retain the :id, it is the same.
   2) Replaces the entity with matching :id from the table with the
      new entity.
   Returns:
     The database handle back"
  database-type)

(defmulti put-entities!
  "[db entity-kw entities]
   _ATOMICALLY_ puts (as in put-entity!) all entities.
   Returns:
     The database handle back"
  database-type)

(defmulti delete-entity!
  "[db entity-kw id]
   Deletes the entity with id from the table (doesn't complain if not exists)
   Returns:
     The database handle back"
  database-type)

(defmulti delete-entities!
  "[db entity-kw ids]
   _ATOMICALLY_ deletes (as in delete-entity!) entities with ids
   Returns:
     The database handle back"
  database-type)

(defmulti delete-all-entities!
  "[db entity-kw ids]
   _ATOMICALLY_ deletes all entities
   Returns:
     The database handle back"
  database-type)

(defmulti update-entity!
  "[db entity-kw id f & args]
   1) Deletes the old entity identified by id using delete-entity!
   2) Puts the new entity (apply f old-entity args) using put-entity!
   3) 1), 2) happen _ATOMICALLY_
   Note that this function allows change of id for an entity.
   Returns:
     The database handle back"
  database-type)

(defmulti get-entity-by-id
  "[db entity-kw id]
   Returns:
     Entity with id, else nil"
  database-type)

(defmulti get-entities
  "[db entity-kw cursor-opts*]
   Third option cursor-opts is optional.
   It has three keys: order-by, skip, limit
   eg: cursor-opts: {:order-by [[[:a] 1]
                                [[:d] -1]
                                [[:a :b :c] -1]]
                     :skip 500
                     :limit 100}
   order-by is vector of [ks v]
   Returns:
     All entities based on cursor-opts"
  database-type)

(defmulti get-entities-count
  "[db entity-kw]
   Returns the number of entities in the entity-kw table.
   This is much more efficient than calling (count (get-entities ...))"
  database-type)

(defmulti search-entities
  "[db entity-kw search-params cursor-opts*]
   Non DB specific search param
   ----------------------------
   A search-param is [f ks vs]
   (*) f is one of :- 
       (.) IN            Value at ks must be one of vs

       (.) NOT-IN        Value at ks must not be one of vs

       (.) SOME-IN       Value at ks is a list of primitives;
                         atleast one element of the list must be one of vs

       (.) SOME-KEY-IN   Value at ks is a list of maps; ks is actually [ks1 ks2] (see below)
                         atleast one element of the list must be one of vs

       (.) =             Value at ks = vs

       (.) SOME=         Value at ks is a list of primitives;
                         atleast one element of the list must be = to vs

       (.) SOME-KEY=     Value at ks is a list of maps; ks is actually [ks1 ks2] (see below)
                         atleast one element of the list must be = vs

       (.) <=            Value at ks <= vs

       (.) >=            Value at ks >= vs

       (.) <             Value at ks < vs

       (.) >             Value at ks > vs

   (*) ks is [:a :b :c], ks is converted to db specific (if not already):
       - in-memory, ks is not converted to #(get-in % ks)
       - mongodb, ks is converted to a search-doc (dot-form ks)
       NOTE for SOME-KEY fs (SOME-KEY-IN & SOME-KEY=)
       - ks is: [[:a :b] [:c :d]]
         It is assumed that value at [:a :b] is a list of maps with keys [:c :d] instead of a simple value.
         example: {:a {:b [{:c {:d 10}} {:c {:d 11}}]}}
         [\"SOME-KEY=\", [[:a :b] [:c :d]] 10]
         [\"SOME-KEY-IN\", [[:a :b] [:c :d]] [10 12]]

   (*) vs is the value(s) against which to match
       Note that vs is a coll if f is IN, NOT-IN, SOME-IN, SOME-KEY-IN
       Else vs is a simple value
      
   DB specific search param
   ------------------------
   Alternatively in place of [f ks vs] you can directly provide
   (*) for in-mem-db a pred-fn g so that (g e) will be used to
       filter out
   (*) for mongodb a search doc

   cursor-opts is optional (see get-entities)

   Returns:
     All entities satisfying search params and based on cursor-opts"
  database-type)

(defmulti search-entities-count
  "[db entity-kw search-params]
   Returns the number of search results (see search-entities)
   This is much more efficient than calling (count (search-entities ...))"
  database-type)

(defmulti drop-database!
  "[db]
   Drops the database(s). Note: all tables are purged.
   This function is useful only for tests.
   Returns:
     DB handle back"
  database-type)

(defmulti shutdown-db!
  "[db]
   Finishes pending tasks and shuts down the db.
   Returns:
     Nothing specific"
  database-type)

(defn generate-defmethods
  [dispatch-val impl-ns]
  (doseq [multi-name (get-multifns (ns-name *ns*))]
    (let [full-name (symbol (str impl-ns) (str multi-name))]
      (eval
        `(defmethod ~multi-name ~dispatch-val
           [& args#]
           (apply ~full-name args#))))))

(defonce __InMemMM (generate-defmethods :in-memory "ekahau.db.in-memory"))

(defonce __MongoMM (generate-defmethods :mongodb "ekahau.db.mongodb"))

;;  Helpers ;;

(defn create-get-by-id-fn
  [type]
  (fn [db id] (get-entity-by-id db type id)))

(defn assoc-new-entity-ids!
  [db entity-kw entities]
  (map (partial assoc-new-entity-id! db entity-kw) entities))

(defn ensure-default-asset-types!
  [db]
  (if (= 0 (count (get-entities db :asset-types)))
    (put-entities! db :asset-types *default-asset-types*)
    db))

(defn ensure-default-zone-groupings!
  [db]
  (if (< 0 (count (get-entities db :zone-groupings)))
    db
    (put-entities! db :zone-groupings *default-zone-groupings*)))

(defn create-vision-database!
  [db-env]
  (-> (create-database! db-env *vision-persistent-tables* *vision-non-persistent-tables*)
    (ensure-default-asset-types!)
    (ensure-default-zone-groupings!)))

(defn- connected-to-vision-database?!
  [db-env]
  (try
    (shutdown-db! (create-vision-database! db-env))
    true
    (catch Exception e
      (do (error "Error checking db connectivity" e) false))))

(defn get-database-status!
  [db-env]
  {:db-type (:tag db-env)
   :connected-to-DB (connected-to-vision-database?! db-env)})

(defn create-in-memory-vision-database
  []
  (in-mem/create-database! nil nil *vision-tables*))
