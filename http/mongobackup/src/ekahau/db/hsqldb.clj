(ns ekahau.db.hsqldb
  (:require
    [clojure.contrib.sql :as sql])
  (:import
    (java.sql Connection DriverManager)))

(defn- convert-name
  [name]
  (apply str (replace {\- \_} (clojure.core/name name))))

(defn- convert-keyword
  [kw]
  (keyword (convert-name kw)))

(defn- get-tables
  []
  (into []
    (resultset-seq
      (-> ^Connection (sql/connection)
        (.getMetaData)
        (.getTables nil nil nil (into-array ["TABLE" "VIEW"]))))))

(defn create-entity-table
  [name]
  (let [table-name (.toUpperCase ^String (convert-name name))]
    (when-not (seq (filter #(-> % :table_name (= table-name)) (get-tables)))
      (sql/create-table (convert-keyword name) [:id "VARCHAR(32)" "PRIMARY KEY"] [:value "LONGVARCHAR"]))))

(defn destroy-entity-table
  [name]
  (sql/drop-table (convert-keyword name)))

(defn put-entity
  [name entity]
  (let [id (:id entity)]
    (sql/update-or-insert-values (convert-keyword name) ["id=?" id] {:id id :value (pr-str entity)})))

(defn get-entity
  [name id]
  (sql/with-query-results res
    [(format "SELECT id, value FROM %s WHERE id=?" (convert-name name)) id]
    (-> res first :value read-string)))

(defn get-entities
  [name]
  (sql/with-query-results res
    [(format "SELECT id, value FROM %s" (convert-name name))]
    (doall (map #(-> % :value read-string) res))))

(defn delete-entity
  [name id]
  (sql/delete-rows (convert-keyword name) ["id=?" id]))

(defn drop-database
  []
  (sql/do-commands
    "DROP SCHEMA PUBLIC CASCADE"))

(defn shutdown-database
  []
  (sql/do-commands "SHUTDOWN"))
