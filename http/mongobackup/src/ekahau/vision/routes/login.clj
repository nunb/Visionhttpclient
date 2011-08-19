(ns ekahau.vision.routes.login
  (:require
    [ekahau.vision.database :as database]
    ekahau.engine.connection)
  (:use
    [clojure.xml :only [parse]]
    [clojure.contrib.trace :only [trace]]
    [compojure.core]
    [ekahau.vision.routes.session :only [delete-session!]]
    [ekahau.vision.routes.helpers :only [create-resource-not-found-response
                                         create-forbidden-response
                                         with-xml-content-type
                                         create-logged-in-response]]
    [ekahau.xml :only [to-xml-str]]
    [ekahau.vision.engine-service]))

(defn logged-in?
  [request]
  (-> request :session :logged-in))

(defn with-login-check
  [handler]
  (fn [request]
    (if (logged-in? request)
      (handler request)
      (create-forbidden-response request))))

(defn- authenticate!
  [connection]
  (println "In login/authenticate!!!!")
  (ekahau.engine.connection/with-engine-connection connection
    (try
      (ekahau.vision.engine-service/get-user!)
      (catch com.ekahau.common.sdk.EException e
        (if-not (= 401 (.getErrorInt e))
          (throw e)
          false)))))

(defn- get-login!
  [request]
  (println "in login!!!!!!!")
  (if (logged-in? request)
    (create-logged-in-response request)
    (create-resource-not-found-response request)))

(defn credentials-from-request
  [request]
  (let [body (parse (:body request))]
    {:username (get-in body [:attrs :username])
     :password (get-in body [:attrs :password])}))

(defn ensure-user-db-entry
  [db user]
  (ekahau.vision.database/put-entity!
   db :users
   (if-let [existing-user (database/get-entity-by-id db :users (:uid user))]
     (merge existing-user (select-keys user [:role]))
     (assoc user :id (:uid user)))))

(defn- create-login-response
  [request engine connection username password]
  {:status 201
   :body (to-xml-str [:login])
   :session (assoc (:session request)
              :logged-in true
              :connection connection
              :username username
              :password password
              :engine-configuration (assoc (get-in engine [:erc-config])
                                      :user username
                                      :password password))})

(defn- login!
  [request db engine]
  (println "in the login! function the request is " request)
  (let [{:keys [username password]} (credentials-from-request request)
        connection (ekahau.engine.connection/*create-engine-connection* username password)]
    (if-let [user (authenticate! connection)]
      (do
        (ensure-user-db-entry db user)
        (println "going to create login-response") 
        (create-login-response request engine connection username password))
      (create-forbidden-response request))))

(defn- logout!
  [request]
  (delete-session! request)
  (to-xml-str [:logout]))

(defn create-login-routes
  [db engine]
  (println "creating login routes")
  (->
    (routes
      (GET "/login" {:as request} (get-login! request))
      (PUT "/login" {:as request} (login! request db engine))
      (DELETE "/login" {:as request} (logout! request)))
    (with-xml-content-type)))
