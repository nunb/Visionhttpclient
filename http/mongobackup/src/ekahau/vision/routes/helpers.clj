(ns ekahau.vision.routes.helpers
  (:use
    [clojure.string :only [join]]
    [clojure.contrib.logging :only [debug info warn error]]
    [compojure.response :only [render]]
    [ring.util.response :only [response file-response status header content-type]]
    [clojure.contrib.trace]
    [ekahau.engine.connection :only [with-engine-connection]]
    [ekahau.string :only [parse-id pluralize-keyword parse-number]]
    [ekahau.vision.database :as database]
    [ekahau.util :only [clojure-keyword-to-camel convert-keys-to-camel-case select-keys-with-non-nil-values]]
    [ekahau.xml :only [to-xml-str]]
    [clojure.contrib.io :only [reader]]
    [clojure.contrib.str-utils :only [str-join]]
    [clojure.contrib.json :only [read-json json-str]]
    [ekahau.vision.engine-service :only [get-user!]])
  (:import
    java.util.Date
    java.io.File))

;; JSON

(defn to-json [x] (clojure.contrib.json/json-str x))

(defn json-from-request-body
  [request]
  (read-json (reader (:body request))))

;; Middlewares ;;

(defn with-engine-binding
  [handler engine]
  (fn [request]
    (ekahau.engine.connection/with-engine-connection (:erc-connection engine)
      (ekahau.engine.connection/with-create-engine-connection (:erc-config engine)
        (handler request)))))

(defn with-user-engine-binding
  [handler]
  (fn [request]
    (ekahau.engine.connection/with-engine-connection (get-in request [:session :connection])
      (binding [ekahau.engine.stream-connection/*request-emsg-seq*
                (partial ekahau.engine.stream-connection/request-emsg-seq (get-in request [:session :engine-configuration]))]
       (handler request)))))

(defn with-rest-header-to-method
  [handler]
  (fn [request]
    (handler
      (if (= :post (:request-method request))
        (let [{headers :headers} request]
          (if-let [rest-method (get headers "x-vision-rest-method")]
            (condp = rest-method
              "PUT" (assoc request :request-method :put)
              "DELETE" (assoc request :request-method :delete)
              request)
            request))
        request))))

(defn- with-trace
  [handler]
  (fn [request]
    (trace "REQUEST" request)
    (trace "RESPONSE" (handler request))))

(defn with-cache-control
  [handler cache-control-string]
  (fn [request]
    (when-let [response (handler request)]
      (header response "Cache-Control" cache-control-string))))

(defn- log-request!
  [request]
  (debug (str "[ROUTE] " (.toUpperCase (name (:request-method request)))
          " " (:uri request)
          " " (dissoc request :servlet-request :servlet-context :body))))

(defn with-route-logging
  [handler ]
  (fn [request]
    (log-request! request)
    (handler request)))

(defn- with-content-type
  [ctype handler]
  (fn [request]
    (when-let [response (handler request)]
      (content-type response ctype))))

(def with-xml-content-type  (partial with-content-type "text/xml"))
(def with-json-content-type (partial with-content-type "application/json"))
(def with-png-content-type  (partial with-content-type "image/png"))
(def with-csv-content-type  (partial with-content-type "text/csv"))

;; ID path string presentation ;;

(defn parse-id-string 
  [^String s]
  (when s
    (vec (map parse-id (.split s ",")))))

(defn str-id-path
  [id-path]
  (str-join "," id-path))

;; Parsing IDs from route ;;

(defn parse-request-route-id
  ([request]
    (parse-request-route-id request :id))
  ([request id-key]
    (-> request :params id-key parse-id)))

(defn parse-request-route-ids
  [request id-keys]
  (map #(parse-request-route-id request %) id-keys))

;; Route & Entity ;;

(defn get-database-entity-by-route-id 
  [db entity-kw request]
  (when-let [id (parse-request-route-id request)]
    (database/get-entity-by-id db entity-kw id)))

(defn get-attributes-from-entity
  [entity attribute-keys]
  (-> entity
    (select-keys attribute-keys)
    (select-keys-with-non-nil-values)
    convert-keys-to-camel-case))

(defn create-database-entity-from-xml!
  [db xml kw parse-new-entity-fn]
  (dosync
    (let [new-entity (parse-new-entity-fn xml db)]
      (database/put-entity! db kw new-entity)
      new-entity)))

(defn read-entities-prxml 
  [db entity-key attribute-keys sort-key]
  (let [entities-keyword (pluralize-keyword entity-key)
        element-name (clojure-keyword-to-camel entity-key)
        elements-name (clojure-keyword-to-camel entities-keyword)]
    (into [elements-name]
      (map #(vec [element-name (get-attributes-from-entity % attribute-keys)])
        (sort-by sort-key (try
                            (database/get-entities db (pluralize-keyword entity-key))
                            (catch Exception e (error (str (.getMessage e))))))))))

(defn read-entities 
  [db entity-key attribute-keys sort-key]
  (to-xml-str (read-entities-prxml db entity-key attribute-keys sort-key)))

;; Error Responses ;;

(defn- create-error-response
  [request content status-code]
  (status (render content request) status-code))

(defn create-resource-not-found-response
  [request]
  (create-error-response request
    (to-xml-str [:resourceNotFound (select-keys request [:uri])])
    404))

(defn create-json-resource-not-found-response
  [request]
  (create-error-response
   request
   (to-json [:resourceNotFound (select-keys request [:uri])])
   404))

(defn create-bad-request-response
  ([request]
    (create-bad-request-response request
      [:badRequest (select-keys request [:uri])] to-xml-str))
  ([request content content-type-fn]
    (create-error-response request (content-type-fn content) 400)))

(defn create-forbidden-response
  [request]
  (-> (response "<forbidden/>") (content-type "text/xml") (status 403)))

(defn create-no-content-response
  [request]
  (-> (response "") (status 204)))

(defn create-service-unavailable-response
  [request]
  (-> (response "") (status 503)))

(defn create-logged-in-response
  [request]
  (let [user-details (ekahau.engine.connection/with-engine-connection
                       (get-in request [:session :connection])
                       (ekahau.vision.engine-service/get-user!))]
    (response (to-xml-str [:response user-details]))))

(defn public-file-response
  [^String path]
  (file-response path {:root "public"}))

(defn create-page-not-found-response
  "A shortcut to create a '404 Not Found' HTTP response."
  ([]
    (create-page-not-found-response "404.html"))
  ([^String filename]
    (status (public-file-response filename) 404)))

;; Session & User ;;

(defn get-user-by-session
  [db session]
  (let [{user-name :username} session]
    (->> (database/get-entities db :users)
         (filter #(= user-name (:name %)))
         (first))))

(defn get-user-by-request-session
  [db request]
  (get-user-by-session db (:session request)))

(defn get-user-id-by-request-session
  [db request]
  (:id (get-user-by-request-session db request)))

;; Misc ;;

(defmacro with-useful-bindings
  [request m & body]
  (let [impls# {:id `(parse-request-route-id ~request)
                :user `(get-user!)
                :timestamp `(.getTime (Date.))
                :xml-body `(clojure.xml/parse (:body ~request))
                :json-body `(read-json (slurp (:body ~request)))
                :skip `(-> ~request :params :skip parse-number)
                :limit `(-> ~request :params :limit parse-number)}
        let-bindings# (vec
                        (remove nil?
                          (mapcat #(when (%1 m) [(%1 m) (%1 impls#)])
                            [:timestamp :id :user :json-body :xml-body :skip :limit])))]
    `(let ~let-bindings#
       ~@body)))
