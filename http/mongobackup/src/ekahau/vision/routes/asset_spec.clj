(ns ekahau.vision.routes.asset-spec
  (:require
   [clojure.java.io :as io])
  (:use
   [clojure.xml :only [parse]]
   [clojure.contrib.json :only [read-json]]
   [clojure.contrib.trace :only [trace]]
   compojure.core
   [ekahau.vision.database :as database]
   [ekahau.vision.json.asset-spec
    :only [asset-spec-to-json
           parse-json-asset-spec]]
   [ekahau.vision.routes.helpers :only
    [get-user-id-by-request-session
     parse-request-route-id
     to-json
     with-json-content-type]]
   [ekahau.string :only [parse-id parse-boolean]]))

(defn put-entity-with-new-id!
  [db entity-type entity]
  (dosync
   (let [entity (database/assoc-new-entity-id! db entity-type entity)]
     (database/put-entity! db entity-type entity)
     entity)))

(defn- create-asset-spec-from-json!
  [db request]
  (let [content (read-json (io/reader (:body request)))]
    (to-json
     (asset-spec-to-json
      (let [asset-spec (database/assoc-new-entity-id!
                        db :asset-specs
                        (assoc (parse-json-asset-spec content)
                          :user-id
                          (get-user-id-by-request-session db request)))]
        (database/put-entity! db :asset-specs asset-spec)
        asset-spec)))))

(defn- delete-asset-spec-json!
  [db request]
  (database/delete-entity! db :asset-specs (parse-request-route-id request))
  (to-json {:result "OK"}))

(defn- get-user-id-from-request
  [db request]
  (let [user-id-str (-> request :params :id)]
    (if (= "me" user-id-str)
      (get-user-id-by-request-session db request)
      (parse-id user-id-str))))

(defn- get-asset-specs-of-user
  [db request]
  (let [user-id (get-user-id-from-request db request)]
    (to-json (->> (database/get-entities db :asset-specs)
                  (filter #(= user-id (:user-id %)))
                  (map asset-spec-to-json)))))

(defn- update-asset-spec-from-json!
  [db request]
  (let [content (read-json (io/reader (:body request)))]
    (to-json
     (asset-spec-to-json
      (dosync
       (let [spec (assoc (parse-json-asset-spec content)
                    :id (parse-request-route-id request)
                    :user-id (get-user-id-by-request-session db request))]
         (put-entity! db :asset-specs spec)
         spec))))))

(defn create-asset-specification-routes
  [db]
  (->
   (routes
    (GET "/users/:id/assetSpecs.json" {:as request}
      (get-asset-specs-of-user db request))
    (POST "/assetSpecs.json" {:as request}
      (create-asset-spec-from-json! db request))
    (PUT "/assetSpecs/:id.json" {:as request}
      (update-asset-spec-from-json! db request))
    (DELETE "/assetSpecs/:id.json" {:as request}
      (delete-asset-spec-json! db request)))
   (with-json-content-type)))
