(ns ekahau.vision.routes.status-type
  (:require
    [ekahau.vision.database :as database])
  (:use
    [clojure.xml :only [parse]]
    compojure.core
    [ekahau.vision.routes.helpers]
    [ekahau.vision.xml.status-type]
    [ekahau.xml :only [to-xml-str]]))

(defn- get-status-types
  [db]
  (to-xml-str (into [:statusTypes] (map status-type-to-xml (database/get-entities db :status-types)))))

(defn- get-status-type
  [db request]
  (when-let [id (parse-request-route-id request)]
    (when-let [status-type (database/get-entity-by-id db :status-types id)]
      (to-xml-str (status-type-to-xml status-type)))))

(defn- entity-from-request-body
  [request entity-from-xml]
  (-> request :body parse entity-from-xml))

(defn- create-new-entity!
  [db request entity-kw entity-from-xml entity-to-xml]
  (dosync
    (let [new-entity (database/assoc-new-entity-id! db entity-kw (entity-from-request-body request entity-from-xml))]
      (database/put-entity! db entity-kw new-entity)
      (to-xml-str (entity-to-xml new-entity)))))

(defn- create-new-status-type!
  [db request]
  (create-new-entity! db request :status-types status-type-from-xml status-type-to-xml))

(defn create-asset-type-routes
  [db]
  (->
    (routes
      (GET "/statusTypes/:id" {:as request}
        (or (get-status-type db request) (create-page-not-found-response)))
      (GET "/statusTypes" {:as request} (get-status-types db))
      (POST "/statusTypes" {:as request}
        (create-new-status-type! db request)))
    (with-xml-content-type)))
