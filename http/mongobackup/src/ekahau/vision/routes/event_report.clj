(ns ekahau.vision.routes.event-report
  (:require
    [ekahau.vision.event-report-service :as ers])
  (:use
    compojure.core
    ekahau.vision.routes.helpers
    [clojure.contrib.logging :only [error]]
    [clojure.contrib.json :only [read-json]])
  (:import java.util.Date))

(defn- event-report-response-with-params
  [request db]
  (with-useful-bindings request
    {:json-body report-params}
    (try
      (to-json (ers/generate-report-for-some-event-ids db
                 (:event-ids report-params)
                 report-params))
      (catch Exception e
        (error "Error while generating event report" e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- event-report-response
  [request db]
  (try
    (to-json (ers/generate-report-for-all-events db {}))
    (catch Exception e
      (error "Error while generating event report" e)
      (create-bad-request-response request (.getMessage e) to-json))))

(defn create-event-report-routes
  [db]
  (-> (routes
        (POST "/eventReport" {:as request}
          (event-report-response-with-params request db))
        (GET "/eventReport" {:as request}
          (event-report-response request db)))
      (with-json-content-type)))
