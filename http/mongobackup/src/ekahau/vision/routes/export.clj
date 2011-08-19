(ns ekahau.vision.routes.export
  (:require
    [ekahau.vision.export-service :as exs])
  (:use
    compojure.core
    ekahau.vision.routes.helpers
    [clojure.contrib.json :only [read-json]]
    [clojure.contrib.logging :only [debug warn error info]])
  (:import
    [java.io File]))

(defn- export-entities-as-csv
  [& args]
  (File. ^String (apply exs/get-entities-as-csv args)))

(defn- export-entities-response
  [request db all?]
  (with-useful-bindings request
    {}
    (let [body (:body request)
          entity-kw (-> request :params :entity-kw keyword)]
      (try
        (if all?
          (export-entities-as-csv db entity-kw)
          (export-entities-as-csv db entity-kw (:ids (read-json (slurp body)))))
        (catch Exception e
          (error (str "Error while exporting " (name entity-kw) " as csv for body: " body) e)
          (create-bad-request-response request (.getMessage e) str))))))

(defn create-export-routes
  [db]
  (-> (routes
        (POST "/export/csv/:entity-kw" {:as request}
          (export-entities-response request db false))
        (GET "/export/csv/:entity-kw" {:as request}
          (export-entities-response request db true)))
    (with-csv-content-type)))

