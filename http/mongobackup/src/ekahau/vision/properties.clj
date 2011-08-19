(ns ekahau.vision.properties
  (:use
    [clojure.set :only [rename-keys]]
    [clojure.contrib.java-utils :only [read-properties]]
    [ekahau.string :only [parse-int
                          parse-boolean]]))

(defn load-vision-properties!
  []
  (read-properties "conf/vision.properties"))

(defn load-vision-config!
  []
  (let [properties (load-vision-properties!)]
    {:port (parse-int (get properties "vision.port"))
     :status-recheck-frequency (parse-int (get properties "vision.status-recheck-frequency" "5000"))}))

(defn load-erc-config!
  []
  (let [properties (load-vision-properties!)
        get-str #(get properties (str "positioning.engine." %))
        get-int #(parse-int (get-str %))]
    {:host (get-str "host")
     :port (get-int "port")
     :user (get-str "user")
     :password (get-str "password")}))

(defn load-email-config!
  []
  (let [properties (->> (load-vision-properties!)
                     (filter (fn [[k v]] (.startsWith ^String k "mail.")))
                     (map (fn [[k v]] [(keyword k) v]))
                     (into {:mail.sender.address "VisionAdmin"}))]
    (rename-keys properties
      {:mail.smtp.host                :host
       :mail.smtp.port                :port
       :mail.smtp.use-authentication  :use-authentication
       :mail.smtp.username            :username
       :mail.smtp.password            :password
       :mail.sender.address           :sender})))

(defn extract-properties
  [properties key-mapping]
  (-> properties
      (select-keys (keys key-mapping))
      (rename-keys key-mapping)))

(defn load-emergin-config!
  []
  (extract-properties (load-vision-properties!) {"emergin.host" :host
                                                 "emergin.service" :service}))

(defn- parse-db-configs
  [properties db-str]
  (let [key-fn (partial re-matches (re-pattern (str "^" db-str "\\.(.+?)\\.(.+)$")))]
    (reduce
      (fn [ret [k v]]
        (if-let [matches (key-fn k)]
          (update-in ret [(nth matches 1)] assoc (nth matches 2) v)
          ret))
      {}
      properties)))

(defn- db-config->vision-db-env
  [db-type db-config]
  (condp #(.startsWith ^String %2 ^String %1) db-type
    "mongodb" {:tag :mongodb
               :host (get db-config "host")
               :port (parse-int (get db-config "port"))
               :database (get db-config "database")}
    "in-memory" {:tag :in-memory
                 :directory (get db-config "directory")
                 :database (get db-config "database")
                 :create (parse-boolean (get db-config "create"))}))

(defn load-db-config!
  []
  (let [properties (load-vision-properties!)
        default-db (get properties "db.default")]
    (db-config->vision-db-env default-db
      (get (parse-db-configs properties "db") default-db))))

(defn load-test-db-configs!
  []
  (let [properties (load-vision-properties!)]
    (into {}
      (map (fn [[k v]]
             [k (db-config->vision-db-env k v)])
        (parse-db-configs properties "testdb")))))

