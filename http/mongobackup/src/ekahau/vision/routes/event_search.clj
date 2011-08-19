(ns ekahau.vision.routes.event-search
  (:require
    [ekahau.vision.event-search-service :as ess])
  (:use
    compojure.core
    ekahau.vision.routes.helpers
    [clojure.contrib.logging :only [error]]
    [clojure.contrib.json :only [read-json]])
  (:import java.util.Date))

(defn- event-search-response
  [request db]
  (with-useful-bindings request
    {:json-body search-params
     :skip skip :limit limit} 
    (try
      (to-json (ess/search-events db search-params
                 :skip skip :limit limit))
      (catch Exception e
        (error "Error while searching events " e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn create-event-search-routes
  [db]
  (-> (routes
        (POST "/eventSearch" {:as request}
          (event-search-response request db)))
      (with-json-content-type)))
