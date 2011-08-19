(ns ekahau.db.in-memory
  (:require
    [ekahau.UID :as uid]
    [clojure.contrib.sql :as sql]
    [ekahau.db.hsqldb :as db])
  (:use
    [ekahau.util :only [filter-search]]
    [clojure.contrib.logging :only (debug info warn)]
    [clojure.set :only (subset?)]))

(defn- create-in-memory-table
  []
  (ref (sorted-map)))

(defn- create-in-memory-tables 
  [table-kws]
  (apply hash-map (mapcat vector table-kws (repeatedly create-in-memory-table))))

(defn- put-entity-in-in-mem-table
  [table entity]
  (assoc table (:id entity) entity)) 

(defn- put-entity-in-in-mem-table!
  [table-ref entity]
  (dosync
    (alter table-ref put-entity-in-in-mem-table entity)))

(defn- put-entities-in-in-mem-table!
  [table-ref entities]
  (dosync
    (doseq [entity entities]
      (put-entity-in-in-mem-table! table-ref entity))))

(defn- delete-entity-from-in-mem-table
  [table id]
  (dissoc table id))

(defn- delete-entity-from-in-mem-table!
  [table-ref id]
  (dosync
    (alter table-ref delete-entity-from-in-mem-table id)))

(defn- persist!
  [db entity-kw f & args]
  (if-let [persistor-agt (-> db ::persistors (get entity-kw))]
    (send-off persistor-agt (fn [db-env]
                              (try
                                (sql/with-connection db-env
                                  (apply f args))
                                (catch Exception e (.printStackTrace e)))
                              db-env))))

(defn- get-table
  [db entity-kw]
  @(entity-kw db))

(defn valid-table?
  [db entity-kw]
  (contains? db entity-kw))

(defn- def-db-fn
  ([name doc-string attr-map params body]
    (let [db-arg (first params)
          ek-arg (second params)]
      `(defn ~name
         ~doc-string
         ~attr-map
         ~params
         (if-not (valid-table? ~db-arg ~ek-arg)
           (throw (Exception. (str "Can't find " ~ek-arg " table in in-mem db")))
           (~@body)))))
  ([name doc-string params body]
    (def-db-fn name doc-string {} params body))
  ([name params body]
    (def-db-fn name "" params body)))

(defmacro defn-db
  {:private true}
  [& args]
  (apply def-db-fn args))

(defn-db db-type
  [db entity-kw]
  (:tag db))

(defn-db get-next-free-entity-id!
  [db entity-kw]
  (uid/gen))

(defn-db get-entity-by-id
  [db entity-kw id]
  (get (get-table db entity-kw) id))

(defn- order-by-vec-to-comp
  [order-by-vec]
  (fn [e1 e2]
    (loop [order-by-vec order-by-vec]
      (if (seq order-by-vec)
        (let [[ks order] (first order-by-vec)
              comp-ks (* order (compare (get-in e1 ks) (get-in e2 ks)))]
          (if (= 0 comp-ks)
            (recur (rest order-by-vec))
            comp-ks))
        0))))

(defn- cursor-ops
  [entities {:keys [order-by skip limit] :as cursor-opts}]
  (let [entities (if order-by
                   (sort (order-by-vec-to-comp order-by) entities)
                   entities)
        entities (if skip (drop skip entities) entities)
        entities (if (and limit (not= 0 limit)) ; for mongodb limit of 0 is no limit at all
                   (take limit entities) entities)]
    entities))

(defn- find-entities
  ([db entity-kw]
    (find-entities db entity-kw nil))
  ([db entity-kw search-params]
    (find-entities db entity-kw search-params nil))
  ([db entity-kw search-params cursor-opts]
    (cursor-ops (filter-search search-params (vals (get-table db entity-kw)))
      cursor-opts)))

(defn-db get-entities
  [db entity-kw & args]
  (apply find-entities db entity-kw nil args))

(defn-db get-entities-count
  [db entity-kw]
  (count (get-table db entity-kw)))

(defn-db search-entities
  [db entity-kw search-params & args]
  (apply find-entities db entity-kw search-params args))

(defn-db search-entities-count
  [db entity-kw search-params]
  (count (search-entities db entity-kw search-params)))

(defn-db assoc-new-entity-id!
  [db entity-kw entity]
  (if (:id entity)
    (if (uid/is-valid? (:id entity))
      entity
      (do
        (warn (str "Invalid id (replacing with valid one) " entity-kw " " entity))
        (assoc entity :id (get-next-free-entity-id! db entity-kw))))
    (assoc entity :id (get-next-free-entity-id! db entity-kw))))

(defn-db put-entity!
  [db entity-kw entity]
  (do
    (dosync
      (let [entity (assoc-new-entity-id! db entity-kw entity)]
        (put-entity-in-in-mem-table! (entity-kw db) entity)
        (persist! db entity-kw db/put-entity entity-kw entity)))
    db))

(defn-db put-entities!
  [db entity-kw entities]
  (do
    (dosync
      (doseq [entity entities]
        (put-entity! db entity-kw entity)))
    db))

(defn-db delete-entity!
  [db entity-kw id]
  (do
    (dosync
      (delete-entity-from-in-mem-table! (entity-kw db) id)
      (persist! db entity-kw db/delete-entity entity-kw id))
    db))

(defn-db delete-entities!
  [db entity-kw ids]
  (do
    (dosync
      (doseq [id ids]
        (delete-entity! db entity-kw id)))
    db))

(defn-db delete-all-entities!
  [db entity-kw]
  (do
    (dosync
      (let [e-ids (map :id (get-entities db entity-kw))]
        (delete-entities! db entity-kw e-ids)))
    db))

(defn-db update-entity!
  [db entity-kw id f & args]
  (do
    (dosync
      (let [old-entity (get-entity-by-id db entity-kw id)
            new-entity (apply f old-entity args)]
        (delete-entity! db entity-kw id)
        (put-entity! db entity-kw new-entity)))
    db))

(defn drop-table!
  [db entity-kw]
  (if-let [a (persist! db entity-kw db/destroy-entity-table (name entity-kw))]
    (await a))
  (-> db
    (update-in [::persistors] dissoc entity-kw)
    (dissoc entity-kw)))

(defn create-new-table!
  [db entity-kw persistent?]
  (let [db (assoc db entity-kw (create-in-memory-table))]
    (if persistent?
      (let [db (update-in db [::persistors] assoc entity-kw (agent (:db-env db)))]
        (sql/with-connection (:db-env db)
          (db/create-entity-table entity-kw)
          (put-entities! db entity-kw (db/get-entities entity-kw))))
      db)))

(defn- vision-db-env->hsqldb-db-env
  [db-env]
  (when db-env
    {:tag (:tag db-env)
     :classname (or (:classname db-env) "org.hsqldb.jdbcDriver")
     :subprotocol (or (:subprotocol db-env) "hsqldb")
     :subname (or (:subname db-env)
                (str "file:" (:directory db-env)
                  "/" (:database db-env)))
     :create (:create db-env)}))

(defn create-database!
  [db-env persistent-tables non-persistent-tables]
  (let [persistent-tables (set persistent-tables)
        non-persistent-tables (set non-persistent-tables)
        db {:tag :in-memory
            :db-env (vision-db-env->hsqldb-db-env db-env)
            :persistent-tables persistent-tables
            :non-persistent-tables non-persistent-tables}
        db (reduce
             #(create-new-table! %1 %2 true)
             db
             persistent-tables)
        db (reduce
             #(create-new-table! %1 %2 false)
             db
             non-persistent-tables)]
    db))

(defn get-table-names
  [db]
  (concat (:persistent-tables db)
    (:non-persistent-tables db)))

(defn- await-for-persistent-ops!
  [db]
  (doseq [a (-> db ::persistors vals)]
    (send a identity))
  (doseq [a (-> db ::persistors vals)]
    (await a)))

(defn drop-database!
  [db]
  (await-for-persistent-ops! db)
  (let [db-env (:db-env db)]
    (sql/with-connection db-env
      (db/drop-database))
    db))

(defn shutdown-db!
  [db]
  (await-for-persistent-ops! db)
  (sql/with-connection (:db-env db)
    (db/shutdown-database)))
