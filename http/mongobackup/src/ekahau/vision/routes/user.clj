(ns ekahau.vision.routes.user
  (:require
   [ekahau.engine.connection]
   [ekahau.engine.cmd]
   [ekahau.vision.database :as database]
   [ekahau.vision.engine-service])
  (:use
   compojure.core
   [ekahau.vision.routes.helpers :only
    [get-user-by-request-session
     get-user-id-by-request-session
     parse-request-route-id
     to-json
     with-xml-content-type
     with-json-content-type]]
   [ekahau.string :only [parse-id]]
   [ekahau.xml :only [to-xml-str]]))

(defn create-get-users-xml
  []
  (let [users (ekahau.vision.engine-service/list-users!)]
    (-> [:users]
      (into (map (fn [user] [:user user]) users))
      (to-xml-str))))

(defn get-me-json
  [db request]
  (to-json
   (select-keys
    (get-user-by-request-session db request)
    [:id :name :role])))

(defn create-user-routes
  [db]
  (routes
   (->
    (routes
     (GET "/users" {:as request} (create-get-users-xml)))
    (with-xml-content-type))
   (->
    (routes
     (GET "/users.json" {:as request}
       (to-json (ekahau.vision.engine-service/list-users!)))
     (GET "/users/me.json" {:as request}
       (get-me-json db request)))
    (with-json-content-type))))
