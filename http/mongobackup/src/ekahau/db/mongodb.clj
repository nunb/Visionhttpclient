(ns ekahau.db.mongodb
  (:require
    [ekahau.UID :as uid]
    [ekahau.db.in-memory :as in-mem])
  (:use
    [clojure.string :only [split-lines split triml trimr]]
    [clojure.contrib.trace :only [trace]]
    [clojure.contrib.logging :only [debug info warn]])
  (:import
    [com.mongodb Mongo DB DBCollection BasicDBObject BasicDBList DBObject DBCursor WriteConcern]
    [java.util List Map]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-table-names
  [db]
  (concat (:persistent-tables db)
    (in-mem/get-table-names (:in-mem-db db))))

(defn- persistent?
  [db entity-kw]
  (contains? (:persistent-tables db) entity-kw))

(defn- in-mem?
  [db entity-kw]
  (in-mem/valid-table? (:in-mem-db db) entity-kw))

(defn valid-table?
  [db entity-kw]
  (or
    (persistent? db entity-kw)
    (in-mem? db entity-kw)))

(defn- drop-table!
  [db entity-kw]
  (cond
    (persistent? db entity-kw) (do
                                 (.drop ^DBCollection
                                         (.getCollection ^DB (:mongodb db) ^String (name entity-kw)))
                                 db)
    (in-mem? db entity-kw) (assoc db :in-mem-db
                             (in-mem/drop-table! (:in-mem-db db) entity-kw))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- mongo-fn
  ([name doc-string attr-map params body]
    (let [[db entity-kw] params
          [mongo mongodb coll-name coll] ['mongo 'mongodb 'coll-name 'coll]
          in-mem-fn (symbol "in-mem" (str name))
          in-mem-invocation (if (= '& (last (butlast params)))
                              (concat
                                (list 'apply in-mem-fn '(:in-mem-db db))
                                (butlast (butlast (rest params)))
                                (list (last params)))
                              (concat
                                (list in-mem-fn '(:in-mem-db db))
                                (rest params)))]
      `(defn ~name
         ~doc-string
         ~attr-map
         ~params
         (cond
           (persistent? ~db ~entity-kw) (let [~(with-meta mongo {:tag 'Mongo}) (:mongo ~db)
                                              ~(with-meta mongodb {:tag 'DB}) (:mongodb ~db)
                                              ~(with-meta coll-name {:tag 'String}) (when ~entity-kw (name ~entity-kw))
                                              ~(with-meta coll {:tag 'DBCollection}) (when ~coll-name
                                                                                       (.getCollection ~mongodb ~coll-name))]
                                         (~@body))
           (in-mem? ~db ~entity-kw) (let [ret# ~in-mem-invocation]
                                      (if (= ret# (:in-mem-db ~db))
                                        ~db
                                        ret#))
           :else (throw (Exception.(str "Can't find " ~entity-kw " table in mongodb")))))))
  ([name doc-string params body]
    (mongo-fn name doc-string {} params body))
  ([name params body]
    (mongo-fn name "" params body)))

(defmacro defn-mongo
  {:private true}
  [& args]
  (apply mongo-fn args))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; convert vision id to mongo id
(defn- convert-id
  [m]
  (let [m (into {} m)]
    (if (contains? m :id)
      (let [id (:id m)]
        (-> m
          (assoc "_id" id)
          (dissoc :id)))
      m)))

; convert mongo id to vision id
(defn- inv-convert-id
  [m]
  (if (contains? m :_id)
    (let [id (:_id m)]
      (-> m
        (assoc :id id)
        (dissoc :_id)))
    m))

; vision entity keys to mongodb keys
; XXX TO-DO : this might be very bad for perf.
(defn- convert-keys
  [m]
  (cond
    (sequential? m) (into []
                      (map convert-keys m))
    (map? m) (zipmap
               (map name (keys m))
               (map convert-keys (vals m)))
    :else m))

; vision mongodb keys to entity keys
; XXX TO-DO : this might be very bad for perf.
(defn- inv-convert-keys
  [m]
  (cond
    (sequential? m) (into []
                      (map inv-convert-keys m))
    (map? m) (zipmap
               (map keyword (keys m))
               (map inv-convert-keys (vals m)))
    :else m))

; clojure {} to com.mongodb.DBObject
; note nil m throws NPE which is fine n expected
(defn- to-DO
  [^Map m]
  (BasicDBObject. m))

; com.mongodb.DBObject to clojure {}
; XXX TO-DO : this might be very bad for perf.
(defn- inv-to-DO
  [o]
  (cond
    (instance? List o) (into []
                         (map inv-to-DO (seq o)))
    (instance? Map o) (let [m (into {} (.toMap ^DBObject o))]
                        (zipmap
                          (keys m)
                          (map inv-to-DO (vals m))))
    :else o))

; Vision entity -> Mongo object
(def DO (comp to-DO convert-keys convert-id))

; Mongo object -> Vision entity
(def INVDO (comp inv-convert-id inv-convert-keys inv-to-DO))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn-mongo db-type
  [db entity-kw]
  (:tag db))

(defn-mongo get-entity-by-id
  [db entity-kw id]
  (INVDO (.findOne coll ^DBObject (DO {:id id}))))

(def search-fn-to-mongo-fn
  ^{:private true}
  {"IN"          "$in"
   "NOT-IN"      "$nin"
   "SOME-IN"     "$in" ; $in on arrays works as SOME-IN!
   "SOME-KEY-IN" "$in" ; $in on arrays of maps works as SOME-KEY-IN!
   "="           :direct
   "SOME="       :direct
   "SOME-KEY="   :direct
   "<="          "$lte"
   ">="          "$gte"
   "<"           "$lt"
   ">"           "$gt"})

(defn- vision-ks-to-mongo-dot
  [ks]
  (apply str (interpose "." (map name (if (= [:id] ks)
                                        [:_id] ks)))))

(defn- search-params-to-doc
  "Logic is:
   1. the mongodb impl will automatically convert simple search params
      like ['IN' 'a.b.c' [1 2 3]] to {'a.b.c' : {'$in' : [1 2 3]}}
      or like [>= 'a.b.c' 42] to {'a.b.c' : {'$gte' : 42}} 
   2. So if your query is a simple form as above then pass on [f ks vs]
      to the mongodb impl
   3. But if your query is complex you can provide the whole search document
      yourself, like for example if you want to use '$elemMatch'
   4. The mongodb impl will check if the search param is a vector?,
      if it is, it will do 1. above, if not it will assume that you
      have passed a search document and directly use that."
  [search-params]
  (DO
    (apply merge-with
      (fn [v1 v2]
        (if (every? map? [v1 v2])
          (merge v1 v2)
          v2))
      {}
      (map (fn [sp]
             (if (vector? sp)
               (let [[^String f ks vs] sp
                     ks (if (.startsWith f "SOME-KEY")
                          (vec (apply concat ks))
                          ks)
                     ks-dot (vision-ks-to-mongo-dot ks)]
                 (if-let [mf (get search-fn-to-mongo-fn f)]
                   (if (= :direct mf)
                     {ks-dot vs}
                     {ks-dot {mf vs}})
                   (throw (Exception. (str "no corresponding mongo fn for search fn " f)))))
               sp))
        search-params))))

(defn- order-by-vec-to-DO
  [order-by-vec]
  (reduce
    (fn [^DBObject order-by-DO [ks order]]
      (.put order-by-DO (vision-ks-to-mongo-dot ks) order)
      order-by-DO)
    (BasicDBObject.)
    order-by-vec))

(defn- mcursor
  [^DBCursor cursor {:keys [order-by skip limit] :as cursor-opts}]
  (let [cursor (if order-by
                 (.sort cursor (order-by-vec-to-DO order-by))
                 cursor)
        cursor (if skip (.skip cursor skip) cursor)
        cursor (if limit (.limit cursor limit) cursor)]
    cursor))

(defn- cursor-to-seq
  [^DBCursor cursor]
  (map INVDO (seq (.toArray cursor))))

(defn- mfind
  ([coll]
    (mfind coll nil))
  ([coll search-params]
    (mfind coll search-params nil))
  ([coll search-params cursor-opts]
    (mcursor (.find ^DBCollection coll (search-params-to-doc search-params))
      cursor-opts)))

(defn-mongo get-entities
  [db entity-kw & args]
  (cursor-to-seq (apply mfind coll nil args)))

(defn-mongo get-entities-count
  [db entity-kw]
  (.count coll))

(defn-mongo search-entities
  [db entity-kw search-params & args]
  (cursor-to-seq (apply mfind coll search-params args)))

(defn-mongo search-entities-count
  [db entity-kw search-params]
  (.count ^DBCursor (mfind coll search-params)))

(defn-mongo get-next-free-entity-id!
  [db entity-kw]
  (uid/gen))

(defn-mongo assoc-new-entity-id!
  [db entity-kw entity]
  (if (:id entity)
    (if (uid/is-valid? (:id entity))
      entity
      (do
        (warn (str "Invalid id (replacing with valid one) " entity-kw " " entity))
        (assoc entity :id (get-next-free-entity-id! db entity-kw))))
    (assoc entity :id (get-next-free-entity-id! db entity-kw))))

(def DEFAULT_WRITE_CONCERN WriteConcern/SAFE)

(defn-mongo put-entity!
  [db entity-kw entity]
  (do
    (.save coll
      ;(trace (DO (assoc-new-entity-id! db entity-kw entity)))
      (DO (assoc-new-entity-id! db entity-kw entity))
      DEFAULT_WRITE_CONCERN)
    db))

; XXX TO-DO this is not ATOMIC
(defn-mongo put-entities!
  [db entity-kw entities]
  (reduce #(put-entity! %1 entity-kw %2) db entities))

(defn-mongo delete-entity!
  [db entity-kw id]
  (do
    (.remove coll (DO {:id id}) DEFAULT_WRITE_CONCERN)
    db))

; XXX TO-DO this is not ATOMIC
(defn-mongo delete-entities!
  [db entity-kw ids]
  (reduce #(delete-entity! %1 entity-kw %2) db ids))

(defn-mongo delete-all-entities!
  [db entity-kw]
  (do
    (.drop coll)
    db))

; XXX TO-DO this is not ATOMIC
(defn-mongo update-entity!
  [db entity-kw id f & args]
  (let [old-entity (get-entity-by-id db entity-kw id)
        new-entity (apply f old-entity args)]
    (delete-entity! db entity-kw id)
    (put-entity! db entity-kw new-entity)))

(defn drop-database!
  [db]
  (.dropDatabase ^DB (:mongodb db))
  db)

; NPE bug fixed in 2.3!
(defn shutdown-db!
  [db]
  (.close ^Mongo (:mongo db)))

(defn- idx->DO
  [idx]
  (let [^DBObject dobj (DO nil)]
    (doseq [[k v] (partition 2 idx)]
      (.put dobj k v))
    dobj))

(defn ensure-indexes!
  [^DB mongodb]
  (loop [ilines (split-lines (slurp "resources/mongodb.indexes"))
         curr-coll nil]
    (when-let [iline (first ilines)]
      (if (= "" (triml iline))
        (recur (rest ilines) nil)
        (if (.startsWith iline " ")
          (let [[idx opts] (map read-string (split iline #","))]
            (.ensureIndex curr-coll (idx->DO idx) (DO opts))
            (recur (rest ilines) curr-coll))
          (recur (rest ilines) (.getCollection mongodb (trimr iline))))))))

(defn create-database!
  [{:keys [^String host ^Integer port database] :as db-env} persistent-tables non-persistent-tables]
  (let [persistent-tables (set persistent-tables)
        non-persistent-tables (set non-persistent-tables)
        mongo (Mongo. host port)
        mongodb (.getDB mongo database)]
    (.getCollectionNames mongodb) ;checking connection
    (ensure-indexes! mongodb)
    {:tag :mongodb
     :db-env db-env
     :persistent-tables persistent-tables
     :in-mem-db (in-mem/create-database! nil nil non-persistent-tables)
     :mongo mongo
     :mongodb mongodb}))
